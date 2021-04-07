package app_kvHttp.model.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import shared.messages.KVMessage.StatusType;

import java.util.Map;

/**
 * See {@link StatusType}
 */
@SuppressWarnings("unused")
public class Status {
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
        final ObjectMapper objectMapper = new ObjectMapper();

        // 1. Generate test case
        final String json = objectMapper.writeValueAsString(Map.of("status", "SERVER_STOPPED"));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final Status obj = objectMapper.readValue(json, Status.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + objectMapper.writeValueAsString(obj));
    }
}
