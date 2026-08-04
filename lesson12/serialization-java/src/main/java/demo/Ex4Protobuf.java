package demo;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

/**
 * §5 — Protobuf. Binary, fields identified by number, not name (see the
 * leading tag byte 0x0a = field 1). The descriptor is built in code here,
 * but the wire format is identical to generated Protobuf. Pure in-memory.
 */
public class Ex4Protobuf {

    public static void main(String[] args) {
        Descriptors.Descriptor d = Utils.protoDescriptor();
        OrderCreated order = Utils.sample();

        byte[] bytes = Utils.protoSerialize(order, d);
        System.out.println("proto descriptor: OrderCreated { order_id=1, amount=2 }");
        System.out.println("Protobuf bytes (" + bytes.length + "): " + Utils.hex(bytes));

        DynamicMessage back = Utils.protoDeserialize(bytes, d);
        System.out.println("round-trip: order_id=" + back.getField(d.findFieldByNumber(1))
                + " amount=" + back.getField(d.findFieldByNumber(2)));
    }
}
