package com.example.outbox.consumer;

import com.example.outbox.config.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 이벤트를 받는 쪽(예: 배송, 알림 서비스) 역할.
 * 실제로 어떤 이벤트가 도착했는지 확인할 수 있도록 메모리에 쌓아 둔다.
 */
@Slf4j
@Component
public class OrderEventListener {

    private final List<ReceivedEvent> receivedEvents = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = {KafkaTopics.ORDER_CREATED_DIRECT, KafkaTopics.ORDER_CREATED_OUTBOX})
    public void listen(ConsumerRecord<String, String> record) {
        log.info("이벤트 수신 (topic={}, key={}): {}", record.topic(), record.key(), record.value());
        receivedEvents.add(new ReceivedEvent(record.topic(), record.key(), record.value()));
    }

    public List<ReceivedEvent> getReceivedEvents() {
        return List.copyOf(receivedEvents);
    }

    public void clear() {
        receivedEvents.clear();
    }

    public boolean hasReceived(String topic, Long orderId) {
        return receivedEvents.stream()
                .anyMatch(e -> e.topic().equals(topic) && e.orderId().equals(orderId.toString()));
    }

    public record ReceivedEvent(String topic, String orderId, String payload) {
    }
}
