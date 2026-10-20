package com.example.notificationservice.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Ban sao contract cua topic orders.events (owner: order-service).
 * Thuc te nen dung chung qua Schema Registry hoac 1 thu vien contract.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    private String eventId;
    private OrderEventType eventType;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    private String orderId;
    private String customerId;
    private String email;
    private String product;
    private int quantity;
    private BigDecimal amount;
}
