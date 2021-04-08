package app_kvHttp.model.request;

import app_kvHttp.model.Model;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class Remapping extends Model {
    private final Pattern find;
    private final String replace;

    /**
     * JSON deserializer hook for Jackson
     */
    @JsonCreator
    public Remapping(
            @JsonProperty(value = "find", required = true) String find,
            @JsonProperty(value = "replace", required = true) String replace
    ) {
        this.find = Pattern.compile(find);
        this.replace = replace;
    }

    /**
     * Getter required for JSON serialization
     */
    public Pattern getFind() {
        return find;
    }

    /**
     * Getter required for JSON serialization
     */
    public String getReplace() {
        return replace;
    }

    /**
     * De/Serialization test
     */
    public static void main(String[] args) throws Exception {
        // 1. Generate test case
        final String json = Model.toString(Map.of("find", "needle", "replace", "new_value"));
        System.out.println("test = " + json);

        // 2. Deserialize from JSON
        final Remapping obj = Model.fromString(json, Remapping.class);
        System.out.println("serialized = " + obj);

        // 3. Serialize into JSON
        System.out.println("deserialized = " + Model.toString(obj));
    }
}