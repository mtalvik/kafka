package demo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * §7 — schema evolution. A field (source) is added on the reader side.
 * Avro resolves the writer schema (v1) against the reader schema (v2) and
 * supplies the declared default — a controlled outcome. JSON has no schema,
 * so the missing field just becomes null: it tolerates the change, but
 * nothing declared or checked it. This is the coexistence problem retention
 * creates, and why Schema Registry (lesson 11) manages compatibility.
 * Pure in-memory.
 */
public class Ex7Evolution {

    public static void main(String[] args) {
        OrderCreated order = Utils.sample();

        System.out.println("--- Avro ---");
        Schema v1 = Utils.avroSchema(Utils.AVRO_V1);
        Schema v2 = Utils.avroSchema(Utils.AVRO_V2);
        byte[] writtenV1 = Utils.avroSerialize(order, v1);           // producer on v1
        System.out.println("wrote with v1 (orderId, amount)");
        GenericRecord resolved = Utils.avroDeserialize(writtenV1, v1, v2);  // reader on v2
        System.out.println("read  with v2 (orderId, amount, source[default=\"unknown\"])");
        System.out.println("resolved: orderId=" + resolved.get("orderId")
                + " amount=" + resolved.get("amount")
                + " source=" + resolved.get("source"));

        System.out.println();
        System.out.println("--- JSON ---");
        byte[] json = Utils.jsonSerialize(order);                    // v1 shape
        System.out.println("wrote v1 " + new String(json));
        OrderCreatedV2 back = Utils.jsonDeserialize(json, OrderCreatedV2.class);
        System.out.println("read into v2 shape: orderId=" + back.orderId
                + " amount=" + back.amount + " source=" + back.source);
    }

    /** v2 reader shape for JSON: the added field is simply absent -> null. */
    public static class OrderCreatedV2 {
        public String orderId;
        public double amount;
        public String source;
    }
}
