package shared.messages;

public interface KVAdminMessage {

    public enum AdminStatusType {
        EMPTY,              /* Creating a new node state */
        INIT,               /* Initialize KVServer with associated Metadata */
        INIT_ACK,           /* Ack INIT procedure */
        START,              /* Start the KVServer */
        START_ACK,          /* Ack the start procedure */
        STOP,               /* Stop the KVServer, client requests rejected. Accept ECS requests */
        STOP_ACK,           /* Ack the stop process */
        SHUTDOWN,           /* Exits the KVServer application */
        SHUTDOWN_ACK,       /* Ack the shutdown process */
        LOCK,               /* Lock the KVServer for all write operations */
        LOCK_ACK,           /* Ack lock procedure */
        UNLOCK,             /* Unlock the KVServer for all write operations */
        UNLOCK_ACK,         /* Ack the unlock procedure */
        MOVE_DATA,          /* Move data from a specified range to the specified port */
        MOVE_DATA_ACK,      /* Ack Move data procedure */
        TRANSFER_REQ,       /* Request server the data is going to be transferred to */
        TRANSFER_REQ_ACK,   /* Responds with details on transfer request port */
        TRANSFER_BEGIN,     /* Begin transfer */
        TRANSFER_COMPLETE   /* Transfer between servers was successful */
    }

    /**
     * @return the original sender associated with this message
     */
    public String getSender();

    /**
     * @return the status string used to identify the request type and response type
     */
    public AdminStatusType getStatus();

    /**
     * @return the value associated with this message
     */
    public String getValue();

    /**
     * @return when the status type is MOVE_DATA, the following returns
     * the data range that must be moved
     */
    public String[] getRange();

    /**
     * @return when the status type is MOVE_DATA, the following returns
     * the address to which the associated range must be moved to
     */
    public String getAddress();

    /**
     * @return byte representation of the following proto
     */
    public byte[] getBytes();

}
