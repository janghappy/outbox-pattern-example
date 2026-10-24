package com.example.outbox.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Outbox Relay 스케줄링. 테스트에서는 꺼 두고 Relay를 직접 호출한다.
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "outbox.relay.scheduling-enabled", havingValue = "true", matchIfMissing = true)
public class SchedulingConfig {
}
