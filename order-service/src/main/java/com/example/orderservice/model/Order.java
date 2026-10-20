package com.example.orderservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String orderId;
    private String customerId;
    private String email;
    private String product;
    private int quantity;
    private BigDecimal amount;
    private OrderStatus status;
    private Instant createdAt;
}
