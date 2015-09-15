package monkey.mikeyo.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import monkey.mikeyo.kafka.BogusObject;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * class to simply serialize a BogusObject.
 */
public class BogusObjectSerializer implements Serializer<BogusObject> {
    private static final ObjectWriter ow = new ObjectMapper().writer();

    public void configure(Map<String, ?> map, boolean b) {
        // do nothing
    }

    public byte[] serialize(String s, BogusObject bogusObject) {
        try {
            // write object to json
            final String serialized = this.ow.writeValueAsString(bogusObject);
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
