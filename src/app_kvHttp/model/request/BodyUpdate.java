package app_kvHttp.model.request;

import app_kvHttp.model.Model;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@SuppressWarnings("unused")
public class BodyUpdate extends Model {
    private final Query filter;
    private final Remapping mapping;

    /**
     * JSON deserializer hook for Jackson
     */
    @JsonCreator
    public BodyUpdate(
            @JsonProperty(value = "filter", required = true) Query filter,
            @JsonProperty(value = "mapping", required = true) Remapping mapping
    ) {
        this.filter = filter;
        this.mapping = mapping;
    }

    /**
     * Getter required for JSON serialization
     */
    public Query getFilter() {
        return filter;
    }

    /**
     * Getter required for JSON serialization
     */
    public Remapping getMapping() {
        return mapping;
    }

    /**
     * De/Serialization test
     */
    public static void main(String[] args) throws Exception {
        // 1. Generate test case
        final String json = Model.toString(Map.of(
                "filter", Map.of("value_filter", "value_.*"),
                "mapping", Map.of("value", "new_value")
        ));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final BodyUpdate obj = Model.fromString(json, BodyUpdate.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + Model.toString(obj));
    }
}
