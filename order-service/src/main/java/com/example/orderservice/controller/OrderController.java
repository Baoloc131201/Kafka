package com.example.orderservice.controller;

import com.example.orderservice.dto.CreateOrderRequest;
import com.example.orderservice.model.OrderEvent;
import com.example.orderservice.producer.OrderEventProducer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderEventProducer producer;

    @PostMapping
    public ResponseEntity<OrderEvent> createOrder(@Valid @RequestBody CreateOrderRequest req) {
        OrderEvent event = OrderEvent.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId(req.getCustomerId())
                .email(req.getEmail())
                .product(req.getProduct())
                .quantity(req.getQuantity())
                .amount(req.getAmount())
                .createdAt(Instant.now())
                .build();
        producer.send(event);
        return ResponseEntity.accepted().body(event);
    }

}
