package com.example.outbox.scenario.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Outbox 테이블에서 미발행 이벤트를 읽어 Kafka로 발행하는 별도 Publisher (Polling Publisher).
 * <p>
 * 발행에 실패한 이벤트는 publishedAt이 null로 남아 있으므로 다음 주기에 다시 발행된다.
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class OutboxRelay {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelayString = "${outbox.relay.fixed-delay-ms}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findTop100ByPublishedAtIsNullOrderByIdAsc();

        for (OutboxEvent event : pendingEvents) {
            try {
                // 브로커의 ack를 받을 때까지 기다린 뒤에 발행 완료로 표시한다
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // 순서를 지키기 위해 여기서 멈추고, 남은 이벤트는 다음 주기에 재시도한다
                log.warn("Outbox 이벤트 발행 실패, 다음 주기에 재시도 (id={}): {}", event.getId(), e.getMessage());
                return;
            }
            event.markPublished();
            log.info("Outbox 이벤트 발행 완료 (id={}, topic={})", event.getId(), event.getTopic());
        }
    }
}
