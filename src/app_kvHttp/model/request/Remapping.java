package app_kvHttp.model.request;

import app_kvHttp.model.Model;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@SuppressWarnings("unused")
public class Remapping extends Model {
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
        // 1. Generate test case
        final String json = Model.toString(Map.of("value", "new_value"));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final Remapping obj = Model.fromString(json, Remapping.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + Model.toString(obj));
    }
}