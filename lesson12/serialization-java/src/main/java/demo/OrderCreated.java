package demo;

/**
 * The one domain event used across every exercise: the same object,
 * serialized three ways, so the formats are compared on equal terms.
 *
 * Public fields + a no-arg constructor keep Jackson happy without
 * annotations. orderId and amount mirror the fields shown for Avro and
 * Protobuf in the lecture.
 */
public class OrderCreated {

    public String orderId;
    public double amount;

    public OrderCreated() {}

    public OrderCreated(String orderId, double amount) {
        this.orderId = orderId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "OrderCreated{orderId=" + orderId + ", amount=" + amount + "}";
    }
}
