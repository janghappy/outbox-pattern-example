package com.example.outbox.scenario.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /** 미발행 이벤트를 저장 순서대로 조회 */
    List<OutboxEvent> findTop100ByPublishedAtIsNullOrderByIdAsc();
}
