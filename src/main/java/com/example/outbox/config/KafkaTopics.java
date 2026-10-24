package com.example.outbox.config;

public final class KafkaTopics {

    /** direct-publish: 서비스가 Kafka로 직접 발행 */
    public static final String ORDER_CREATED_DIRECT = "order-created.direct";

    /** outbox: Outbox Relay가 발행 */
    public static final String ORDER_CREATED_OUTBOX = "order-created.outbox";

    private KafkaTopics() {
    }
}
