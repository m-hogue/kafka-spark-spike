package monkey.mikeyo.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import monkey.mikeyo.kafka.RandomObject;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * A simple {@link RandomObject} serializer.
 */
public class RandomObjectSerializer implements Serializer<RandomObject> {

    private static final ObjectWriter ow = new ObjectMapper().writer();

    public void configure(Map<String, ?> map, boolean b) {
        // do nothing
    }

    public byte[] serialize(String s, RandomObject randomObject) {
        try {
            // write object to json
            final String serialized = this.ow.writeValueAsString(randomObject);
            System.out.println("serialized object: " + serialized);
            return serialized.getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close() {
        // do nothing
    }
}
