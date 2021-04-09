package app_kvHttp.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

/**
 * Base/utility class to make the {@link ObjectMapper} API easier to use
 */
public class Model {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String toString(Object obj) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(obj);
    }

    public static <T> T fromString(String input, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(input, clazz);
    }

    public static byte[] toRaw(Object obj) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(obj);
    }

    public static <T> T fromRaw(InputStream input, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(input, clazz);
    }
}
