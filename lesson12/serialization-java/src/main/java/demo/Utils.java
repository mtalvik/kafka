package demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Shared helpers: the SASL/PLAIN connection block (for the two exercises
 * that touch the broker) and serialize/deserialize helpers for all three
 * formats — JSON (Jackson), Avro (GenericRecord), Protobuf (DynamicMessage).
 *
 * No code generation anywhere: the Avro schema is parsed from a string and
 * the Protobuf descriptor is built in code. Both produce bytes identical to
 * their generated-code equivalents.
 */
final class Utils {

    private Utils() {}

    static final String SER_DEMO = "ser-demo";

    static OrderCreated sample() {
        return new OrderCreated("A-1001", 42.5);
    }

    // ---- connection (Ex1, Ex8 only) ----

    /** SASL/PLAIN connection block from client.properties. */
    static Properties connectionProps() {
        Properties props = new Properties();
        String path = System.getProperty("client.properties.path", "client.properties");
        try (FileInputStream in = new FileInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("failed to load " + path, e);
        }
        return props;
    }

    // ---- JSON (Jackson) ----

    static final ObjectMapper MAPPER = new ObjectMapper();

    static byte[] jsonSerialize(Object o) {
        try {
            return MAPPER.writeValueAsBytes(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static <T> T jsonDeserialize(byte[] data, Class<T> type) {
        try {
            return MAPPER.readValue(data, type);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ---- Avro (GenericRecord, no codegen) ----

    static final String AVRO_V1 =
            "{\"type\":\"record\",\"name\":\"OrderCreated\",\"namespace\":\"demo\"," +
            "\"fields\":[" +
            "{\"name\":\"orderId\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}]}";

    // v2 adds an optional field with a default — the key to safe evolution.
    static final String AVRO_V2 =
            "{\"type\":\"record\",\"name\":\"OrderCreated\",\"namespace\":\"demo\"," +
            "\"fields\":[" +
            "{\"name\":\"orderId\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}," +
            "{\"name\":\"source\",\"type\":\"string\",\"default\":\"unknown\"}]}";

    static Schema avroSchema(String json) {
        return new Schema.Parser().parse(json);
    }

    static byte[] avroSerialize(OrderCreated o, Schema schema) {
        GenericRecord rec = new GenericData.Record(schema);
        rec.put("orderId", o.orderId);
        rec.put("amount", o.amount);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        try {
            new GenericDatumWriter<GenericRecord>(schema).write(rec, enc);
            enc.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    /** Deserialize with an explicit writer and reader schema — this is what
     *  performs schema resolution (filling reader defaults). */
    static GenericRecord avroDeserialize(byte[] data, Schema writer, Schema reader) {
        BinaryDecoder dec = DecoderFactory.get().binaryDecoder(data, null);
        try {
            return new GenericDatumReader<GenericRecord>(writer, reader).read(null, dec);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ---- Protobuf (DynamicMessage, no .proto / protoc) ----

    /** Build the OrderCreated descriptor in code: order_id=1, amount=2. */
    static Descriptors.Descriptor protoDescriptor() {
        DescriptorProtos.DescriptorProto msg = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("OrderCreated")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("order_id").setNumber(1)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING).build())
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("amount").setNumber(2)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE).build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("order.proto").setSyntax("proto3").addMessageType(msg).build();
        try {
            return Descriptors.FileDescriptor
                    .buildFrom(file, new Descriptors.FileDescriptor[]{})
                    .findMessageTypeByName("OrderCreated");
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }

    static byte[] protoSerialize(OrderCreated o, Descriptors.Descriptor d) {
        return DynamicMessage.newBuilder(d)
                .setField(d.findFieldByNumber(1), o.orderId)
                .setField(d.findFieldByNumber(2), o.amount)
                .build()
                .toByteArray();
    }

    static DynamicMessage protoDeserialize(byte[] data, Descriptors.Descriptor d) {
        try {
            return DynamicMessage.parseFrom(d, data);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    // ---- printing ----

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x ", x));
        }
        return sb.toString().trim();
    }
}
