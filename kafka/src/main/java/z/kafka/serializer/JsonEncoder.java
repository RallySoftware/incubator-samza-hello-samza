package z.kafka.serializer;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JsonEncoder implements Encoder<Object> {
    public JsonEncoder(VerifiableProperties properties) {
    }

    @Override
    public byte[] toBytes(final Object t) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(t).getBytes();
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }
}
