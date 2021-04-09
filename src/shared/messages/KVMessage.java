package shared.messages;

public interface KVMessage {
	
	public enum StatusType {
		GET, 					/* Get - request */
		GET_ERROR, 				/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 			/* requested tuple (i.e. value) found */
		PUT, 					/* Put - request */
		PUT_SUCCESS, 			/* Put - request successful, tuple inserted */
		PUT_UPDATE, 			/* Put - request successful, i.e. value updated */
		PUT_ERROR, 				/* Put - request not successful */
		DELETE_SUCCESS, 		/* Delete - request successful */
		DELETE_ERROR,			/* Delete - request successful */
		FAILED,					/* Unknown Error */

		SERVER_STOPPED, 		/* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK,		/* Server locked for write, only get possible */
		SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */

		GET_ALL,				/* Get all key/value associated with expression */
		GET_ALL_SUCCESS,		/* requested tuple (i.e. value) found */
		GET_ALL_ERROR,			/* requested tuple (i.e. value) not found */
		PUT_ALL,				/* Put all values associated with each key expression */
		PUT_ALL_SUCCESS,		/* Put all - request successful, tuple(s) inserted */
		PUT_ALL_ERROR,			/* Put all - request not successful */
		DELETE_ALL,				/* Delete all key's associated with expression */
		DELETE_ALL_SUCCESS,		/* Delete all - request successful */
		DELETE_ALL_ERROR,		/* Delete all - request failed */

		COORDINATE_GET_ALL,		    	/* Designate server to coordinate get request across servers */
		COORDINATE_GET_ALL_SUCCESS, 	/* Coordinate get - request successful, ie. tuple(s) found */
		COORDINATE_GET_ALL_ERROR,		/* Coordinate get - request not successful */
		COORDINATE_PUT_ALL,				/* Designate server to coordinate put request across servers */
		COORDINATE_PUT_ALL_SUCCESS, 	/* Coordinate put - request successful, ie. tuple(s) inserted */
		COORDINATE_PUT_ALL_ERROR,   	/* Coordinate put - request not successful */
		COORDINATE_DELETE_ALL,			/* Coordinate delete all - request */
		COORDINATE_DELETE_ALL_SUCCESS,  /* Coordinate delete all - request successful */
		COORDINATE_DELETE_ALL_ERROR		/* Coordinate delete all error - request failed */
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}


