package com.example.orderservice.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Contract cua topic orders.events. Moi su kien thay doi trang thai don hang la 1 message.
 * - eventId: dinh danh duy nhat cua event -> consumer dung de chong xu ly trung
 * - eventType: loai su kien -> consumer tu quyet dinh xu ly hay bo qua
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

    public static OrderEvent of(OrderEventType type, Order order) {
        return OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(type)
                .occurredAt(Instant.now())
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .email(order.getEmail())
                .product(order.getProduct())
                .quantity(order.getQuantity())
                .amount(order.getAmount())
                .build();
    }
}
