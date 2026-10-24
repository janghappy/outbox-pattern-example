package com.example.outbox.scenario.directpublish;

import com.example.outbox.common.SimulatedFailureException;
import com.example.outbox.config.KafkaTopics;
import com.example.outbox.consumer.OrderEventListener;
import com.example.outbox.order.Order;
import com.example.outbox.order.OrderRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * direct-publish: DB insert & Kafka 직접 발행
 */
@SpringBootTest(properties = "outbox.relay.scheduling-enabled=false")
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class DirectPublishOrderServiceTest {

    @Autowired
    DirectPublishOrderService directPublishOrderService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OrderEventListener orderEventListener;

    @Test
    void 정상이면_주문_저장과_이벤트_발행이_모두_된다() {
        Order order = directPublishOrderService.placeOrder("키보드", false);

        assertThat(orderRepository.findById(order.getId())).isPresent();
        await().atMost(Duration.ofSeconds(10))
                .until(() -> orderEventListener.hasReceived(KafkaTopics.ORDER_CREATED_DIRECT, order.getId()));
    }

    @Test
    void insert_이후_장애가_나면_주문은_저장됐지만_이벤트는_누락된다() {
        long before = orderRepository.count();

        assertThatThrownBy(() -> directPublishOrderService.placeOrder("모니터", true))
                .isInstanceOf(SimulatedFailureException.class);

        // 주문은 이미 커밋되어 DB에 남아 있다
        assertThat(orderRepository.count()).isEqualTo(before + 1);
        Long orderId = orderRepository.findAll().getLast().getId();

        // 하지만 이벤트는 끝내 도착하지 않는다 → 주문 데이터와 이벤트 불일치
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5))
                .until(() -> !orderEventListener.hasReceived(KafkaTopics.ORDER_CREATED_DIRECT, orderId));
    }
}
