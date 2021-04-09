package app_kvHttp.model.request;

import app_kvHttp.model.Model;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@SuppressWarnings("unused")
public class BodySelect extends Model {
    private final Query filter;

    /**
     * JSON deserializer hook for Jackson
     */
    @JsonCreator
    public BodySelect(@JsonProperty(value = "filter", required = true) Query filter) {
        this.filter = filter;
    }

    /**
     * Getter required for JSON serialization
     */
    public Query getFilter() {
        return filter;
    }

    /**
     * De/Serialization test
     */
    public static void main(String[] args) throws Exception {
        // 1. Generate test case
        final String json = Model.toString(Map.of("filter", Map.of("value_filter", "value_.*")));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final BodySelect obj = Model.fromString(json, BodySelect.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + Model.toString(obj));
    }
}
