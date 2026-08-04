package demo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * §5 — Avro. The schema is JSON, but the bytes are binary and carry no
 * field names — only a length-prefixed string and a packed double. The
 * decoder needs the schema to make sense of them. Pure in-memory.
 */
public class Ex3Avro {

    public static void main(String[] args) {
        Schema schema = Utils.avroSchema(Utils.AVRO_V1);
        OrderCreated order = Utils.sample();

        byte[] bytes = Utils.avroSerialize(order, schema);
        System.out.println("Avro schema (JSON): " + schema);
        System.out.println("Avro bytes (" + bytes.length + "): " + Utils.hex(bytes));

        GenericRecord back = Utils.avroDeserialize(bytes, schema, schema);
        System.out.println("round-trip: orderId=" + back.get("orderId")
                + " amount=" + back.get("amount"));
    }
}
