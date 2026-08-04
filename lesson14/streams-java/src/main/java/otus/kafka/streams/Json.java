package otus.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/** Minimal Jackson-backed JSON serde factory. */
final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {
    }

    static <T> Serde<T> serde(Class<T> type) {
        Serializer<T> serializer = (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        };
        Deserializer<T> deserializer = (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, type);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
