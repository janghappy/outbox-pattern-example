package com.example.outbox.scenario.outbox;

import com.example.outbox.common.SimulatedFailureException;
import com.example.outbox.config.KafkaTopics;
import com.example.outbox.order.Order;
import com.example.outbox.order.OrderCreatedEvent;
import com.example.outbox.order.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * outbox: 주문 insert와 Outbox insert를 하나의 DB 트랜잭션으로 묶는다.
 * <p>
 * Kafka 발행은 이 서비스가 하지 않고 {@link OutboxRelay}가 Outbox 테이블을 읽어서 한다.
 * 그래서 주문과 이벤트는 "둘 다 저장되거나 둘 다 저장되지 않는다".
 */
@RequiredArgsConstructor
@Service
public class OutboxOrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public Order placeOrder(String productName, boolean failAfterInsert) {
        // 1. DB insert: 트랜잭션 안이므로 아직 커밋되지 않는다
        Order order = orderRepository.save(new Order(productName));

        // 2. direct-publish와 같은 위치의 장애: 커밋 전이므로 1의 주문 insert도 롤백된다
        if (failAfterInsert) {
            throw new SimulatedFailureException();
        }

        // 3. Outbox insert: Kafka 대신 같은 트랜잭션으로 이벤트를 저장하고, 발행은 OutboxRelay가 한다
        outboxEventRepository.save(new OutboxEvent(
                "Order", order.getId().toString(), KafkaTopics.ORDER_CREATED_OUTBOX, OrderCreatedEvent.from(order).toString()));
        return order;
    }
}
