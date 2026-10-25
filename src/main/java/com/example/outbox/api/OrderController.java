package com.example.outbox.api;

import com.example.outbox.common.SimulatedFailureException;
import com.example.outbox.consumer.OrderEventListener;
import com.example.outbox.consumer.OrderEventListener.ReceivedEvent;
import com.example.outbox.order.Order;
import com.example.outbox.order.OrderRepository;
import com.example.outbox.scenario.directpublish.DirectPublishOrderService;
import com.example.outbox.scenario.outbox.OutboxEvent;
import com.example.outbox.scenario.outbox.OutboxEventRepository;
import com.example.outbox.scenario.outbox.OutboxOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RequiredArgsConstructor
@RestController
public class OrderController {

    private final DirectPublishOrderService directPublishOrderService;
    private final OutboxOrderService outboxOrderService;
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OrderEventListener orderEventListener;

    /** direct-publish: DB insert 후 Kafka 직접 발행 */
    @PostMapping("/direct-publish/orders")
    public Order placeOrderWithDirectPublish(@RequestBody OrderRequest request,
                                             @RequestParam(defaultValue = "false") boolean failAfterInsert) {
        return directPublishOrderService.placeOrder(request.productName(), failAfterInsert);
    }

    /** outbox: DB insert + Outbox insert, 발행은 Relay가 */
    @PostMapping("/outbox/orders")
    public Order placeOrderWithOutbox(@RequestBody OrderRequest request,
                                      @RequestParam(defaultValue = "false") boolean failAfterInsert) {
        return outboxOrderService.placeOrder(request.productName(), failAfterInsert);
    }

    @GetMapping("/orders")
    public List<Order> orders() {
        return orderRepository.findAll();
    }

    @GetMapping("/outbox-events")
    public List<OutboxEvent> outboxEvents() {
        return outboxEventRepository.findAll();
    }

    @GetMapping("/received-events")
    public List<ReceivedEvent> receivedEvents() {
        return orderEventListener.getReceivedEvents();
    }

    /** 상태 페이지의 초기화 버튼: 주문, Outbox, 수신 이벤트를 모두 비운다 */
    @DeleteMapping("/data")
    public void reset() {
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
        orderEventListener.clear();
    }

    @ExceptionHandler(SimulatedFailureException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public String handleSimulatedFailure(SimulatedFailureException e) {
        return e.getMessage();
    }

    public record OrderRequest(String productName) {
    }
}
