package app_kvHttp.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class Query {
    private final Pattern keyFilter;
    private final Pattern valueFilter;

    /**
     * JSON deserializer hook for Jackson
     */
    @JsonCreator
    public Query(
            @JsonProperty(value = "key_filter") String keyFilter,
            @JsonProperty(value = "value_filter") String valueFilter
    ) {
        keyFilter = keyFilter == null ? "" : keyFilter;
        valueFilter = valueFilter == null ? "" : valueFilter;
        if (keyFilter.isEmpty() && valueFilter.isEmpty()) {
            throw new IllegalArgumentException("At least one filter must be specified");
        }
        this.keyFilter = Pattern.compile(keyFilter);
        this.valueFilter = Pattern.compile(valueFilter);
    }

    /**
     * Getter required for JSON serialization
     */
    public Pattern getKeyFilter() {
        return keyFilter;
    }

    /**
     * Getter required for JSON serialization
     */
    public Pattern getValueFilter() {
        return valueFilter;
    }

    /**
     * De/Serialization test
     */
    public static void main(String[] args) throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // 1. Generate test case
        final String json = objectMapper.writeValueAsString(Map.of("value_filter", "value_.*"));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final Query obj = objectMapper.readValue(json, Query.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + objectMapper.writeValueAsString(obj));
    }
}
