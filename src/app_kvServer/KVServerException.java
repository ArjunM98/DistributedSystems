package app_kvServer;

import shared.messages.KVMessage.StatusType;

/**
 * Custom exception class to return more meaningful messages from {@link KVServer}
 */
public class KVServerException extends Exception {
    private final StatusType statusType;

    public KVServerException(String message, Throwable cause, StatusType statusType) {
        super(message, cause);
        this.statusType = statusType;
    }

    public KVServerException(String message, StatusType statusType) {
        super(message);
        this.statusType = statusType;
    }

    public StatusType getErrorCode() {
        return this.statusType;
    }
}
