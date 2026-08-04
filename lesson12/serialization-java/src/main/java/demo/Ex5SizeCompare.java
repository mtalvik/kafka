package demo;

import com.google.protobuf.Descriptors;
import org.apache.avro.Schema;

/**
 * §6 — the same OrderCreated in three formats: real byte sizes and the
 * cost of serializing 100k times. This is the concrete version of the
 * text-vs-binary table — not "usually bigger," but this many bytes.
 * Pure in-memory.
 */
public class Ex5SizeCompare {

    public static void main(String[] args) {
        OrderCreated order = Utils.sample();
        Schema avro = Utils.avroSchema(Utils.AVRO_V1);
        Descriptors.Descriptor proto = Utils.protoDescriptor();

        int jsonLen = Utils.jsonSerialize(order).length;
        int avroLen = Utils.avroSerialize(order, avro).length;
        int protoLen = Utils.protoSerialize(order, proto).length;

        int iters = 100_000;
        long jsonMs = time(() -> Utils.jsonSerialize(order), iters);
        long avroMs = time(() -> Utils.avroSerialize(order, avro), iters);
        long protoMs = time(() -> Utils.protoSerialize(order, proto), iters);

        System.out.printf("%-10s %6s %16s%n", "format", "bytes", (iters / 1000) + "k serialize");
        System.out.printf("%-10s %6d %13d ms%n", "JSON", jsonLen, jsonMs);
        System.out.printf("%-10s %6d %13d ms%n", "Avro", avroLen, avroMs);
        System.out.printf("%-10s %6d %13d ms%n", "Protobuf", protoLen, protoMs);
        System.out.printf("%nJSON is ~%.1fx the Avro payload; field names are the difference.%n",
                (double) jsonLen / avroLen);
    }

    private static long time(Runnable r, int iters) {
        for (int i = 0; i < 10_000; i++) {
            r.run();               // warm up the JIT
        }
        long t0 = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            r.run();
        }
        return (System.nanoTime() - t0) / 1_000_000;
    }
}
