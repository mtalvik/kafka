package demo;

/**
 * §5 — JSON. Text, self-describing: field names travel in every message.
 * The payload is exactly the readable string. Pure in-memory, no broker.
 */
public class Ex2Json {

    public static void main(String[] args) {
        OrderCreated order = Utils.sample();

        byte[] bytes = Utils.jsonSerialize(order);
        System.out.println("JSON bytes (" + bytes.length + "): " + new String(bytes));

        OrderCreated back = Utils.jsonDeserialize(bytes, OrderCreated.class);
        System.out.println("round-trip: " + back);
    }
}
