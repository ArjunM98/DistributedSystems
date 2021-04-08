package app_kvHttp.model.response;

import app_kvHttp.model.Model;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import shared.messages.KVMessage.StatusType;

import java.util.Map;

/**
 * See {@link StatusType}
 */
@SuppressWarnings("unused")
public class Status extends Model {
    private final StatusType status;

    /**
     * JSON deserializer hook for Jackson
     */
    @JsonCreator
    public Status(@JsonProperty(value = "status", required = true) String status) {
        this(StatusType.valueOf(status));
    }

    /**
     * Constructor for response building
     */
    public Status(StatusType status) {
        this.status = status;
    }

    /**
     * Getter required for JSON serialization
     */
    public StatusType getStatus() {
        return status;
    }

    /**
     * De/Serialization test
     */
    public static void main(String[] args) throws Exception {
        // 1. Generate test case
        final String json = Model.toString(Map.of("status", "SERVER_STOPPED"));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final Status obj = Model.fromString(json, Status.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + Model.toString(obj));
    }
}
