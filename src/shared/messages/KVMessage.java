package shared.messages;

public interface KVMessage {

    public enum StatusType {
        GET, /* Get - request */
        GET_ERROR, /* requested tuple (i.e. value) not found */
        GET_SUCCESS, /* requested tuple (i.e. value) found */
        PUT, /* Put - request */
        PUT_SUCCESS, /* Put - request successful, tuple inserted */
        PUT_UPDATE, /* Put - request successful, i.e. value updated */
        PUT_ERROR, /* Put - request not successful */
        DELETE_SUCCESS, /* Delete - request successful */
        DELETE_ERROR, /* Delete - request successful */
        FAILED, /* Failed - Server Response that something is wrong */
        INVALID_KEY, /* Message size exceeded for key */
        INVALID_VALUE, /* Message size exceeded for value */
        INVALID_FORMAT, /* Message format unknown */

        SERVER_STOPPED, /* Server is stopped, no requests are processed */
        SERVER_WRITE_LOCK, /* Server locked for write, only get possible */
        SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */
        SERVER_NOT_FOUND, /* When we cannot find server responsible for request */
        SERVER_ACTIVE, /* Server is active, requests are processed */
        
        KEYRANGE, /* Keyrange - Request to get keyrange of all nodes */
        KEYRANGE_SUCCESS, /* Respond with keyrange ofr ECS Nodes*/

        REPLICATE,
        REPLICATE_SUCCESS,

        KEYRANGE_READ,
        KEYRANGE_READ_SUCCESS,

        GET_ALL_KEYS,

        SQLCREATE,
        SQLCREATE_SUCCESS,
        SQLCREATE_ERROR,
        
        SQLSELECT,
        SQLSELECT_SUCCESS,
        SQLSELECT_ERROR,

        SQLDROP,
        SQLDROP_SUCCESS,
        SQLDROP_ERROR,

        SQLINSERT,
        SQLINSERT_SUCCESS,
        SQLINSERT_ERROR,
    }

    /**
     * @return the key that is associated with this message,
     *         null if not key is associated.c
     */
    public String getKey();

    /**
     * @return the value that is associated with this message,
     *         null if not value is associated.
     */
    public String getValue();

    /**
     * @return a status string that is used to identify request types,
     *         response types and error types associated to the message.
     */
    public StatusType getStatus();

}
