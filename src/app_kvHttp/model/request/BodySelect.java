package app_kvHttp.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@SuppressWarnings("unused")
public class BodySelect {
    private final int maxResults;
    private final Query filter;

    /**
     * JSON deserializer hook for Jackson
     */
    @JsonCreator
    public BodySelect(
            @JsonProperty(value = "max_results") Integer maxResults,
            @JsonProperty(value = "filter", required = true) Query filter
    ) {
        this.maxResults = maxResults == null ? -1 : maxResults;
        this.filter = filter;
    }

    /**
     * Getter required for JSON serialization
     */
    public int getMaxResults() {
        return maxResults;
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
        final ObjectMapper objectMapper = new ObjectMapper();

        // 1. Generate test case
        final String json = objectMapper.writeValueAsString(Map.of("filter", Map.of("value_filter", "value_.*")));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final BodySelect obj = objectMapper.readValue(json, BodySelect.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + objectMapper.writeValueAsString(obj));
    }
}
