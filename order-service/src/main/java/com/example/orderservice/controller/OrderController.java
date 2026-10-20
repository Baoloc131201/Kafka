package com.example.orderservice.controller;

import com.example.orderservice.dto.CreateOrderRequest;
import com.example.orderservice.model.Order;
import com.example.orderservice.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Order> createOrder(@Valid @RequestBody CreateOrderRequest req) {
        return ResponseEntity.accepted().body(orderService.create(req));
    }

    @PostMapping("/{orderId}/pay")
    public ResponseEntity<Order> pay(@PathVariable String orderId) {
        return ResponseEntity.accepted().body(orderService.pay(orderId));
    }

    @PostMapping("/{orderId}/cancel")
    public ResponseEntity<Order> cancel(@PathVariable String orderId) {
        return ResponseEntity.accepted().body(orderService.cancel(orderId));
    }

    @GetMapping("/{orderId}")
    public Order get(@PathVariable String orderId) {
        return orderService.get(orderId);
    }
}
