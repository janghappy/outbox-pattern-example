package com.example.outbox.scenario.outbox;

import com.example.outbox.common.SimulatedFailureException;
import com.example.outbox.config.KafkaTopics;
import com.example.outbox.consumer.OrderEventListener;
import com.example.outbox.order.Order;
import com.example.outbox.order.OrderRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

/**
 * outbox: DB insert & Outbox insert → Relay가 발행
 * <p>
 * 스케줄링을 끄고 Relay를 직접 호출해서 발행 시점을 테스트에서 제어한다.
 */
@SpringBootTest(properties = "outbox.relay.scheduling-enabled=false")
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OutboxOrderServiceTest {

    @Autowired
    OutboxOrderService outboxOrderService;

    @Autowired
    OutboxRelay outboxRelay;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    OrderEventListener orderEventListener;

    // 자동 구성된 KafkaTemplate 빈은 KafkaTemplate<?, ?> 타입이라 raw 타입으로 spy한다
    @SuppressWarnings("rawtypes")
    @MockitoSpyBean
    KafkaTemplate kafkaTemplate;

    @Test
    void 주문과_Outbox가_함께_저장되고_Relay가_발행한다() {
        Order order = outboxOrderService.placeOrder("키보드", false);

        // 서비스는 Kafka에 발행하지 않고 Outbox에 미발행 이벤트만 남긴다
        OutboxEvent event = findOutboxEvent(order);
        assertThat(event.isPublished()).isFalse();

        // 별도 Publisher(Relay)가 Outbox를 읽어 발행한다
        outboxRelay.publishPendingEvents();

        assertThat(findOutboxEvent(order).isPublished()).isTrue();
        await().atMost(Duration.ofSeconds(10))
                .until(() -> orderEventListener.hasReceived(KafkaTopics.ORDER_CREATED_OUTBOX, order.getId()));
    }

    @Test
    void insert_이후_장애가_나면_주문이_롤백되어_주문도_이벤트도_없다() {
        long ordersBefore = orderRepository.count();
        long eventsBefore = outboxEventRepository.count();

        assertThatThrownBy(() -> outboxOrderService.placeOrder("모니터", true))
                .isInstanceOf(SimulatedFailureException.class);

        // 주문도 이벤트도 남지 않는다 → 불일치 없음
        assertThat(orderRepository.count()).isEqualTo(ordersBefore);
        assertThat(outboxEventRepository.count()).isEqualTo(eventsBefore);
    }

    @Test
    void Kafka_발행에_실패해도_Outbox에_남아_있다가_다음_주기에_발행된다() {
        Order order = outboxOrderService.placeOrder("마우스", false);

        // 첫 번째 발행 시도: Kafka 장애
        doThrow(new KafkaException("브로커 연결 실패 (시뮬레이션)"))
                .doCallRealMethod()
                .when(kafkaTemplate).send(anyString(), anyString(), anyString());
        outboxRelay.publishPendingEvents();

        // 이벤트는 사라지지 않고 미발행 상태로 남는다
        assertThat(findOutboxEvent(order).isPublished()).isFalse();

        // 두 번째 발행 시도: Kafka 복구 → 발행 성공
        outboxRelay.publishPendingEvents();

        assertThat(findOutboxEvent(order).isPublished()).isTrue();
        await().atMost(Duration.ofSeconds(10))
                .until(() -> orderEventListener.hasReceived(KafkaTopics.ORDER_CREATED_OUTBOX, order.getId()));
    }

    private OutboxEvent findOutboxEvent(Order order) {
        return outboxEventRepository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(order.getId().toString()))
                .findFirst()
                .orElseThrow();
    }
}
