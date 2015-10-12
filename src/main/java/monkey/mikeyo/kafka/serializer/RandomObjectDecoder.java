package monkey.mikeyo.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import monkey.mikeyo.kafka.RandomObject;

import java.io.IOException;

public class RandomObjectDecoder implements Decoder<RandomObject> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public RandomObjectDecoder(VerifiableProperties props) {

    }

    @Override
    public RandomObject fromBytes(byte[] bytes) {
        try {
            RandomObject ro = objectMapper.readValue(bytes, RandomObject.class);
            System.out.println("deserialized random object: " + ro.toString());
            return ro;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
