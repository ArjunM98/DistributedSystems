package ecs;

public interface IECSNode {

    public enum ServerStatus {
        OFFLINE,    // Server is offline
        INACTIVE,   // SSH start call has been given, but a response has not been received
        STARTING,   // Server is in the processing of starting up
        RUNNING,    // Server is actively accepting read and write requests
        STOPPED,    // Server is not running
        STOPPING    // Server is in the process of stopping
    }

    /**
     * @return the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

    /**
     * @return status of server
     */
    public ServerStatus getNodeStatus();

    /**
     *
     * @param status - New ServerStatus
     */
    public void setNodeStatus(ServerStatus status);
}
