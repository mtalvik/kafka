// producer-json/src/main/java/com/example/producerjson/PriceEvent.java
package com.example.producerjson;

public class PriceEvent {
    public String orderId;
    public double price;

    public PriceEvent() {}

    public PriceEvent(String orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }
}