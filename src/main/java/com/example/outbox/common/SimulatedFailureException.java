package com.example.outbox.common;

/**
 * DB insert 이후에 발생하는 장애(서버 다운, 네트워크 단절, 버그 등)를 흉내 내는 예외.
 */
public class SimulatedFailureException extends RuntimeException {

    public SimulatedFailureException() {
        super("DB insert 이후 장애 발생 (시뮬레이션)");
    }
}
