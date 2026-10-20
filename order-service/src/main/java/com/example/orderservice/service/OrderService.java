package com.example.orderservice.service;

import com.example.orderservice.dto.CreateOrderRequest;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderEvent;
import com.example.orderservice.model.OrderEventType;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Luu don hang trong bo nho (demo). Thuc te se luu DB va dung Transactional Outbox
 * de dam bao "ghi DB" va "gui event" khong bi lech nhau.
 */
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderEventProducer producer;
    private final Map<String, Order> orders = new ConcurrentHashMap<>();

    public Order create(CreateOrderRequest req) {
        Order order = Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId(req.getCustomerId())
                .email(req.getEmail())
                .product(req.getProduct())
                .quantity(req.getQuantity())
                .amount(req.getAmount())
                .status(OrderStatus.CREATED)
                .createdAt(Instant.now())
                .build();
        orders.put(order.getOrderId(), order);
        producer.send(OrderEvent.of(OrderEventType.ORDER_CREATED, order));
        return order;
    }

    public Order pay(String orderId) {
        Order order = changeStatus(orderId, OrderStatus.CREATED, OrderStatus.PAID);
        producer.send(OrderEvent.of(OrderEventType.ORDER_PAID, order));
        return order;
    }

    public Order cancel(String orderId) {
        Order order = changeStatus(orderId, OrderStatus.CREATED, OrderStatus.CANCELLED);
        producer.send(OrderEvent.of(OrderEventType.ORDER_CANCELLED, order));
        return order;
    }

    public Order get(String orderId) {
        Order order = orders.get(orderId);
        if (order == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found: " + orderId);
        }
        return order;
    }

    private synchronized Order changeStatus(String orderId, OrderStatus expected, OrderStatus next) {
        Order order = get(orderId);
        if (order.getStatus() != expected) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Order " + orderId + " is " + order.getStatus() + ", expected " + expected);
        }
        order.setStatus(next);
        return order;
    }
}
