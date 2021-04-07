package app_kvHttp.model.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import shared.messages.KVMessage;

import java.util.Map;

/**
 * See {@link app_kvServer.storage.IKVStorage.KVPair}
 */
@SuppressWarnings("unused")
public class KV {
    private final String key;
    private final String value;

    /**
     * JSON deserializer hook for Jackson
     */
    @JsonCreator
    public KV(
            @JsonProperty(value = "key", required = true) String key,
            @JsonProperty(value = "value") String value
    ) {
        this.key = key;
        this.value = value == null ? "" : value;
    }

    /**
     * Constructor for response building
     */
    public KV(KVMessage message) {
        this(message.getKey(), message.getValue());
    }

    /**
     * Getter required for JSON serialization
     */
    public String getKey() {
        return key;
    }

    /**
     * Getter required for JSON serialization
     */
    public String getValue() {
        return value;
    }

    /**
     * De/Serialization test
     */
    public static void main(String[] args) throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // 1. Generate test case
        final String json = objectMapper.writeValueAsString(Map.of("key", "key_1"));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final KV obj = objectMapper.readValue(json, KV.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + objectMapper.writeValueAsString(obj));
    }
}
