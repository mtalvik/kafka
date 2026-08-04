package demo;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * §3 — the key-partitioning hazard. The default partitioner picks the
 * partition from the SERIALIZED key: toPositive(murmur2(keyBytes)) % n.
 * The same logical key serialized two ways (String "42" vs Long 42)
 * produces different bytes and can land in a different partition — which
 * silently breaks per-key ordering. Pure in-memory (computes what the
 * partitioner would pick against a 3-partition topic).
 */
public class Ex6KeyPartitioning {

    public static void main(String[] args) {
        int partitions = 3;
        StringSerializer stringSer = new StringSerializer();
        LongSerializer longSer = new LongSerializer();

        System.out.printf("%-5s %-22s %-22s %s%n",
                "id", "String-key -> part", "Long-key -> part", "same?");

        int diff = 0;
        for (long id = 40; id <= 49; id++) {
            byte[] asString = stringSer.serialize(Utils.SER_DEMO, Long.toString(id));
            byte[] asLong = longSer.serialize(Utils.SER_DEMO, id);
            int sp = partition(asString, partitions);
            int lp = partition(asLong, partitions);
            boolean same = sp == lp;
            if (!same) {
                diff++;
            }
            System.out.printf("%-5d %-22d %-22d %s%n", id, sp, lp, same ? "yes" : "no");
        }

        System.out.println();
        System.out.println("=> " + diff + "/10 ids land in a DIFFERENT partition depending on key");
        System.out.println("   serialization. Same logical key, different partition -> per-key");
        System.out.println("   ordering broken. Pin one key serializer everywhere.");
    }

    /** Default partitioner for a non-null key. */
    private static int partition(byte[] keyBytes, int n) {
        int hash = org.apache.kafka.common.utils.Utils.murmur2(keyBytes);
        return org.apache.kafka.common.utils.Utils.toPositive(hash) % n;
    }
}
