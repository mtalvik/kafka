package demo;

/**
 * Ex2 — the fix, idempotence ON.
 *
 * Identical demo to Ex1 but enable.idempotence=true. Disrupt the
 * connection the same way (LAB.md); this time the producer attaches
 * PID + sequence number to every record, the broker drops any resend it
 * has already accepted, and no DUPLICATE line ever prints. Ordering is
 * preserved too, so no GAP either.
 *
 * Idempotence forces acks=all, max.in.flight<=5, retries>0 automatically
 * (it is on by default since Kafka 3.0), so Ex2 sets only the flag.
 */
public class Ex2Idempotent {

    public static void main(String[] args) throws Exception {
        Ex1Problem.runIdempotenceDemo(true);
    }
}
