package com.example.outbox.scenario.directpublish;

import com.example.outbox.common.SimulatedFailureException;
import com.example.outbox.config.KafkaTopics;
import com.example.outbox.order.Order;
import com.example.outbox.order.OrderCreatedEvent;
import com.example.outbox.order.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * direct-publish: DB insert 후 Kafka로 직접 발행한다 (Dual Write).
 * <p>
 * DB와 Kafka는 하나의 트랜잭션으로 묶을 수 없기 때문에,
 * insert가 커밋된 뒤 produce 전에 장애가 나면 주문은 저장됐지만 이벤트는 누락된다.
 */
@RequiredArgsConstructor
@Service
public class DirectPublishOrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public Order placeOrder(String productName, boolean failAfterInsert) {
        // 1. DB insert: save()가 끝나면 바로 커밋된다
        Order order = orderRepository.save(new Order(productName));

        // 2. insert와 produce 사이의 장애
        if (failAfterInsert) {
            throw new SimulatedFailureException();
        }

        // 3. Kafka produce: 2에서 예외가 나면 여기까지 오지 못해 이벤트가 누락된다
        kafkaTemplate.send(KafkaTopics.ORDER_CREATED_DIRECT, order.getId().toString(), OrderCreatedEvent.from(order).toString());
        return order;
    }
}
