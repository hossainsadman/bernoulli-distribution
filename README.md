# eagerDB

eagerDB is a lightweight, fast, and fault-tolerant NoSQL distributed database written in Java that requires zero configuration. It is built on top of a data-centric eventual consistency model, and is designed to be scalable with hundreds of clients and servers.

![image (1)](https://github.com/sadmanca/eagerDB/assets/41028402/2cdd9743-dcb2-4de8-a5a3-8378b896c731)

## Supported Operations

### Key-Value Operations

#### Put Key-Value Pair

`put <key> <value>`

    –– puts a key-value pair into the database
    
    e.g.
    ```
    put name Steve
    put age 12
    ```

#### Get Value From Key

`get <key>`

    –– gets the value associated with key
    
    e.g.
    ```
    get name
    get age
    ```

### SQL Operations

#### Creating a Table

`sqlcreate <tablename> <schema>`

    –– creates an empty table with the specified schema
    –– the first column in the schema is taken to be the primary key for the table
        
    e.g.
    ```
    sqlcreate People name:text
    sqlcreate School student:text,age:int
    ```

#### Inserting rows to a table

`sqlinsert <tablename> <row>`
    
    –– inserts a JSON-specified row into tablename
    
    e.g.
    ```
    sqlinsert People {“name":Steve}
    sqlinsert School {"age":12,"student":Jason}
    sqlinsert School {“student":Jason,"age":12}
    ```


#### Updating existing rows in a table

`sqlupdate <tablename> <row>`
    
    –– update a JSON-specified row in tablename
    
    e.g.
    ```
    sqlupdate People {“name":Steve}
    sqlupdate School {"age":12,"student":Jason}
    ```

#### Querying a table (i.e. a select command)

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

#### Dropping an existing table

`sqldrop <tablename>` 
    
    –– drop tablename from database
    
    e.g.
    ```
    sqldrop School
    ```
