package com.example.outbox.order;

/**
 * Kafka로 발행하는 주문 생성 이벤트.
 * 예제라서 직렬화는 하지 않고 record의 toString()을 그대로 payload로 보낸다.
 */
public record OrderCreatedEvent(Long orderId, String productName) {

    public static OrderCreatedEvent from(Order order) {
        return new OrderCreatedEvent(order.getId(), order.getProductName());
    }
}
