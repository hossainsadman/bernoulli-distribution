# eagerDB

**eagerDB** is a lightweight, fast, and fault-tolerant NoSQL distributed database written in Java that requires zero configuration.

- [What is eagerDB?](#what-is-eagerdb)
- [Architecture](#architecture)
- [Building from Source](#building-from-source)
- [CLI Usage](#cli-usage)
- [Database API](#database-api)

## What is eagerDB?

eagerDB is a distributed NoSQL key-value database (with additional support for SQL-like tables) built on top of a data-centric eventual consistency model. It **scales** horizontally (is designed to be scalable to hundreds of clients and servers); **survives** server failures with minimal latency disruption and no manual intervention; supports **eventually-consistent** BASE transactions; uses a **custom-designed** TCP-based internal message protocol for system communication; and provides a **simple key-value API** for structuring, manipulating, and querying data.

![image (1)](https://github.com/sadmanca/eagerDB/assets/41028402/2cdd9743-dcb2-4de8-a5a3-8378b896c731)

## Architecture

For an in-depth discussion of the eagerDB architecture, see the [Architecture Guide](#architecture-guide).

## Building from source

eagerDB is built using [Apache Ant](https://ant.apache.org/). To build eagerDB and its component programs, run:

```bash
ant build
```

## CLI Usage

eagerDB is split into 3 separate applications: `eagerDB-service.jar`, `eagerDB-server`, and `eagerDB-client.jar`. `eagerDB-client.jar` must be running at all times for proper database functionality (before starting up any server, run the service first).

To start up any of the applications using their default configurations, run:

```bash
java -jar eagerDB-*.jar
```

where `*` corresponds to one of `client`, `server`, or `service`. Any number of servers and clients can connect each running instance of the eagerDB-service.

### Optional Command Line Parameters

`eagerDB-server.jar` and `eagerDB-service.jar` can take optional command line parameters for specifying TCP communication and logging properties, including:

- `-h --help`: display help, including application API and optional command line parameters
- `-a --address <<IP ADDRESS>>`: IP address to run the specific application on; defaults to localhost
- `-p --port <<PORT NUMBER>>`: port number to run the specific application on; defaults to 20010
- `-l --logFile <<LOGFILE PATH>>`: file to write application logs to
- `-ll --logLevel <<LOG LEVEL>>`: set log4J level for application (in order descending severity: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL); defaults to INFO
- `-c --cli`: **SERVICE-only** parameter; enables the service CLI for adding and removing single or groups of servers
- `-c --cacheSize`: **SERVER-only** parameter; set the number of key-value pairs to store in server cache; defaults to 10
- `-s --cacheStrategy`: **SERVER-only** parameter; set the cache strategy for a server (one of "FIFO", "LFU", "LRU"); defaults to "FIFO"
- `-d -dir`: **SERVER-only** parameter; set the path for the directory where the server instance will persist it's data; defaults to "db" concatenated with the MD5 hash of the server's host and port number
- `-e -ecsHostAndPort`: **SERVER-only** parameter; set the host and port number for the eagerDB-service instance that the server should connect to; defaults to the default (or provided) server address and port number parameters.
- An optional `ecs_config.json` file can also be used in the local directory to setup configurations for a eagerDB-service and multiple eagerDB-server instances (such that all specific servers start up automatically on eagerDB-service startup).

#### Example Usage

```bash
java -jar eagerDB-service.jar --cli --logLevel WARN
java -jar eagerDB-server.jar --port 1057 --cacheSize 5 --cacheStrategy LRU
java -jar eagerDB-client.jar
```

## Database API

### Supported Operations

#### Key-Value Operations

##### Put Key-Value Pair

`put <key> <value>`

    –– puts a key-value pair into the database
    
    e.g.
    ```
    put name Steve
    put age 12
    ```

##### Get Value From Key

`get <key>`

    –– gets the value associated with key
    
    e.g.
    ```
    get name
    get age
    ```

### SQLTable Operations

##### Creating a table

`sqlcreate <tablename> <schema>`

    –– creates an empty table with the specified schema
    –– the first column in the schema is taken to be the primary key for the table
        
    e.g.
    ```
    sqlcreate People name:text
    sqlcreate School student:text,age:int
    ```

##### Inserting rows to a table

`sqlinsert <tablename> <row>`
    
    –– inserts a JSON-specified row into tablename
    
    e.g.
    ```
    sqlinsert People {“name":Steve}
    sqlinsert School {"age":12,"student":Jason}
    sqlinsert School {“student":Jason,"age":12}
    ```


##### Updating existing rows in a table

`sqlupdate <tablename> <row>`
    
    –– update a JSON-specified row in tablename
    
    e.g.
    ```
    sqlupdate People {“name":Steve}
    sqlupdate School {"age":12,"student":Jason}
    ```

##### Querying a table (i.e. a select command)

`sqlselect <tablename>`
    
    –– select all columns & all rows in tablename
    
    e.g.
    ```
    sqlselect People
    ```


`sqlselect <columns> from <tablename> where <conditions>`
   
    –– select the specified columns (inc. all via *) & rows matching the conditions in tablename
    
    e.g.
    ```
    sqlselect * from People
    sqlselect {student,age} from School 
    sqlselect {student} from School where {age>7,student=Jason}
    ```

##### Dropping an existing table

`sqldrop <tablename>` 
    
    –– drop tablename from database
    
    e.g.
    ```
    sqldrop School
    ```

## Architecture Guide

### Client-Server Connections

The ClientConnection class represents a connection for each distinct client connected to a KVServer. The class utilizes methods from the Communication Service class to retrieve GET and PUT requests from the client. The server keeps track of all the connections as threads, via storing a list of ClientConnection connections. KVServer methods that enable data access and modification (e.g. putKV, getKV) are synchronized to enable multi-threading eliminate possibility of data races with regards to modifying values of key-value pairs. Each ClientConnection waits for client requests in a blocking manner, then processes messages via evaluating the statusType of the request.

### Data Persistance

Key-value pairs are stored permanently on disk: the key is the file name and the value is the content of the file. For simplicity and speed of access, JSON or XML was not used. All respective key-value files for a server are stored in a specific directory (see [optional command line parameters](#optional-command-line-parameters)). The putKV method checks if the value corresponding to a key on the server is null to check if it is deleting a key-value pair, then it checks “/db” to find whether the file named key exists. If not, it will throw an exception (which eventually gets handled by ClientConnection). The getKV method checks if the key is in the cache; if uncache server will then check disk storage. getKV will also throw an exception if the value is invalid (ClientConnection remains fault-tolerant from crashes due to invalid PUT or GET requests). Escape sequences (e.g. `\n`) are supported in eagerDB for both keys and values (all socket messages are delimited by `\r\n`). The CommunicationService class is responsible for recognizing if there is any invalid key or value (or format). Then, the server can append the appropriate message (eg. “Invalid format.”), that will ultimately be sent back.

### Cache

LRU, LFU, and FIFO caches are implemented separately in the Caches class. Caches enable bypassing disk storage access and enable speeding up eagerDB performance on larger cache sizes. When the cache is full, LRU removes the least recently accessed key-value pair. The removeEldestEntry method is overridden such that it is only executed when the size exceeds the capacity. LFU removes the least frequently accessed key value, this is tracked via mapping key to each frequency and vice versa. FIFO utilizes a queue and removes the least recently added key-value pair. 

### Socket Communication

A socket-based communication protocol was implemented to send and receive BasicKVMessages between components. This protocol involves the processes of marshaling and unmarshalling to convert the message objects into byte arrays for transmission and reconstruct them on the receiving end. eagerDB supports two types of message protocols:

- *Local Message Protocol*: the local message protocol outlines the marshaling techniques for KVMessages sent using the eagerDB-client (via the KVStore class). This allows both keys and values to include spaces, new line characters and other special characters.
- *External Message Protocol*: the external message protocol allows external clients to connect to eagerDB-server instances (without having to use an eagerDB-client) and enables marshalling and unmarshalling external client messages gracefully. This byte representation of messages using the external message protocol are as follows (*note*: `[space]` denotes an empty string): `<statustype>[space]<key>[space]<value>`

To differentiate between which protocol to use to marshall and unmarshall the incoming messages through the socket, a 10 byte secret is appended to the local protocol message (messages marshaled from the KVStore). Upon receiving a message on the socket, the server checks for the presence of the secret. If it present, eagerDB unmarshalls it using the local message protocol (using the external message protocol otherwise). Sending and receiving messages has been encapsulated within the MessageService class.

### eagerDB-service

The eagerDB-service instance manages a distributed set of server information and their operational states. This includes when nodes are added or removed from the servers, upon which it adjusts the ranges of key values that servers are responsible for. Also, it handles data transfer requests between servers to maintain consistency of data. The eagerDB-service relies on a MD5 hashing mechanism encapsulated by the ECSHashRing structure (enabling fair distribution of data amongst available nodes). 

The eagerDB-service maintains an ArrayList of ECSNode interface, to ensure that the eagerDB-service has information about each node (including node name and associated  socket). This structure is maintained and utilized in various methods (e.g. `addnodes`). The eagerDB-service actively uses ServerSocket to listen to addresses specified in JSON. The eagerDB-service then can accept connections from KVServers, dynamically appending to the hash ring. This overall procedure allows eagerDB-service to automate the nodes, execute commands accordingly, and organize server/node information. The eagerDB-service also handles shutting down KVServers, altering node availability, and managing configurations such as strategy and cache size.

### Server-Service Connection

The KVServer class supports communication with an Elastic Consistent Hashing (ECS) system to maintain metadata, facilitating its operation within a distributed environment. This connection is primarily realized through the integration of ECSHashRing and ECSNode objects, which enable the server to be aware of the overall structure of the distributed system and its place within that structure. By maintaining a connection to the ECS, the server can dynamically adjust to changes in the system topology, such as nodes joining or leaving, through methods like setHashRing and setMetadata. This allows for efficient distribution and lookup of key-value pairs across the network, adhering to the principles of consistent hashing for load balancing. Furthermore, the server supports direct communication with the eagerDB-service for receiving and transferring key-value pairs when necessary, as indicated by changes to the hash ring. This adaptability not only enhances the scalability and fault tolerance of the system but also ensures a more evenly distributed workload across the network, making the KVServer a crucial component in the distributed key-value storage system.

Each ECSNode in the ECSHashRing has a server socket that corresponds to a specific KVServer. Servers connect to eagerDB-service on initialization, whereupon a new ECSNode object is created and added to the hashring. Following that, a 3-handshake is executed between eagerDB-service and KVServer where the updated hashring and hash ranges in the ECSNode are sent via socket connection. This same socket connection remains open in a thread for transferring KV pairs from a KVServer after a node is added to the hashring; specifically, pairs are sent from the server to eagerDB-service as a middleman, after which they are then sent to the newly added ECSNode.

### Hash Ring of Servers

The ECSHashRing class is designed for managing the distribution of keys across nodes in a distributed system, ensuring an even and efficient distribution through elastic consistent hashing. This class utilizes a `TreeMap<BigInteger, ECSNode>` object to represent the hash ring, where each BigInteger key represents the hash value of a node, and ECSNode is the corresponding node in the network. The hash value of the node is essentially its identifier, which is the hash of the nodes host and port. The TreeMap is chosen for its natural ordering of keys, allowing for efficient lookups, insertions, and deletions, which are essential for maintaining the hash ring's integrity as nodes join and leave the system. The components of the ECSHashRing class include:

- _Serializable Interface_: `... Implements Serializable`, enabling instances of ECSHashRing to be serialized and deserialized, a necessity for distributed systems where state needs to be shared or persisted.
- _TreeMap Structure_: The hash ring is backed by a TreeMap, leveraging its sorted nature to manage the nodes in the ring based on their hash values. This ensures that operations such as finding the correct node for a given key or adding/removing nodes are efficient.
- _Key Distribution_: The class provides mechanisms to evenly distribute keys across the available nodes by mapping each key to a point on the hash ring and then assigning it to the nearest node in the clockwise direction.

#### Hash Ring Metadata

The Metadata is essential to create an end to end flow between the KVClient and the distributed servers and facilitate accurate communication. In the case of the server receiving a key that is not within its hash range, the server would respond with SERVER_NOT_RESPONSIBLE as well as a serialized ECSHashRing to transmit over the socket. The serialized hashring would contain relevant information of all running nodes and their respective hash ranges, so whomever is concerned can correct their calling implementation. 

The KVStore is also configured to assume failure on attempting to send a GET/PUT request to any server. This allows for predicting the arrival of updated metadata, which can be used to reiterate and configure which server socket requests are routed to (the Jacksons library is used to unmarshal and deserialize the JSON metadata).

An important consideration is to make sure that failures do not become a blocker on the client side and the application does not fall into a continuous cycle of updating metadata and sending requests to servers that do not handle certain key ranges. As a result, a configurable max retry count is included which will only attempt to connect to a server a set amount of times. When that limit is reached timeout occurs and responds to the client with an error.

### Data Replication

Each server can either be a coordinator, replica, or not responsible for any possible key. For any possible key, there is a single coordinator server responsible for it and two additional replic servers. Only coordinators are allowed to handle put requests (if a replica receives a put request, it returns SERVER_NOT_RESPONSIBLE and reconnects the client to the coordinator), while all coordinators and replicas are allowed to handle get requests. When coordinators receive put requests, they replicate the key-value pairs to each of their 2 replicas via message status type REPLICATE (and on receiving said messages use the prior built functions for putting key-value pairs). Since replicas always receive new or updated key-value pairs after coordinators, the stored data always has eventual consistency; by simply extending the previous message system for putting pairs into server storage systems (rather than building a new system, our implementation remains simple and easy to understand.

The CommunicationService class uses sockets to connect coordinators with each replica. Intermittent reading from those sockets occurs in order to verify that they haven’t been closed; when an end of file exception occurs in attempting to read from such a socket, a coordinator/replica is detected to have failed and consequently hashring metadata is updated and propagated to all servers in the hashring (including setting of a new coordinator/replica for the next successor node after the current triple chain of coordinator/replica/replica).

#### Data Replication Protocol

When a coordinator receives a PUT command for a key it is directly responsible for, it will apply its changes locally first, and then using the Replicator class, which maintains connections with its replicas, it propagates the same change over the socket connection as a REPLICATE command. When the replicas receive the command on the socket (which will eventually occur), it will then apply the changes locally and reflect the state of the coordinator server up to a particular point in time.

### eagerDB-service / eagerDB-server Communication

The communication between ECS and KVServer uses a simple socket connection along with a simple protocol to send and receive messages in a rudimentary fashion. The ECSMessage class defines the different message types, which essentially serve to communicate the updated state of the ECS configuration to a server, or command a server to update its state to reach a certain desired state which is consistent among all servers.

The removal of servers follows the same protocol as discussed above. Adding servers to the ECS configuration is also very straightforward. Whenever a new node is added to the hashring, all servers in the ring are notified with an updated hash ring. The successor of the newly added node will be notified of its hash range being changed as well, which will trigger it to redistribute its key value pairs and transfer keys it is not responsible for over the KVServer <-> KVServer communication line. Since all the servers will receive an updated hashring, they will make sure they do not contain any key value pairs which they are not responsible for. If there are such keys, it will handle them accordingly (deleting/transferring/replicating).

### SQL Tables

Internally, tables are represented as SQLTable objects with instance variables that contain the table data, including the table’s name, columns, column types (string, int), and rows (stored as a collection of HashMaps). Similar to key-value pairs, tables are uniquely identified by their name and use the same messaging system, with table names indicated by keys and table contents as JSON strings in values. The entire contents (all rows & attributes) of a table are stored on a coordinator server (identified by the MD5 hash of that table’s name) and replicated across the same two replicas as with keys on all mutation requests (i.e. table creation/deletion, row insertion/update).

Currently only “string” and “int” data types are supported for columns, and the condition field only supports greater than (>), less than (<), and equals (=) conditions. Extra whitespace is permitted between keywords for SQL commands (e.g. `sqldrop   School`) and within, preceding, or succeeding table names, column names, or conditions (based on positioning, the whitespace may be considered as part of a table or column name).

#### SQL Table Replication & Failure Recovery

Rather than transferring the entire data contents of tables for replication of SQL table mutation from coordinators to replicas, which can be inefficient and nonperformant for larger tables, the specific SQL request (e.g. SQLINSERT) received by a coordinator is passed on to its replicas (with an indication that the request is for replication only—e.g. SQLINSERT_REPLICATE—and so that request isn’t passed on to servers that the replica itself is a coordinator for). Conversely, for rebalancing keys and tables on the removal or failure of a server the entire table contents are sent as there is no prior data present for those specific tables in newly appointed replicas

