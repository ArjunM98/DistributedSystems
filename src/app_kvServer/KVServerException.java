package app_kvServer;

import shared.messages.KVAdminMessage.AdminStatusType;
import shared.messages.KVMessage.StatusType;

/**
 * Custom exception class to return more meaningful messages from {@link KVServer}
 */
public class KVServerException extends Exception {
    private final StatusType statusType;
    private final AdminStatusType adminStatusType;

    public KVServerException(String message, Throwable cause, StatusType statusType) {
        super(message, cause);
        this.adminStatusType = null;
        this.statusType = statusType;
    }

    public KVServerException(String message, StatusType statusType) {
        super(message);
        this.adminStatusType = null;
        this.statusType = statusType;
    }

    public KVServerException(String message, Throwable cause, AdminStatusType adminStatusType) {
        super(message, cause);
        this.statusType = null;
        this.adminStatusType = adminStatusType;
    }

    public KVServerException(String message, AdminStatusType adminStatusType) {
        super(message);
        this.statusType = null;
        this.adminStatusType = adminStatusType;
    }

    public StatusType getErrorCode() {
        return this.statusType;
    }
}
