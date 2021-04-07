package app_kvHttp.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@SuppressWarnings("unused")
public class Remapping {
    private final String value;

    /**
     * JSON deserializer hook for Jackson
     */
    @JsonCreator
    public Remapping(@JsonProperty(value = "value", required = true) String value) {
        this.value = value;
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
        final String json = objectMapper.writeValueAsString(Map.of("value", "new_value"));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final Remapping obj = objectMapper.readValue(json, Remapping.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + objectMapper.writeValueAsString(obj));
    }
}