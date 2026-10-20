package com.example.notificationservice.service;

import com.example.notificationservice.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final JavaMailSender mailSender;

    @Value("${app.mail.from}")
    private String from;

    @Value("${app.mail.enabled}")
    private boolean mailEnabled;

    public void notifyCustomer(OrderEvent event) {
        log.info("[notify] eventType={} customer={} email={} orderId={} product={} x{} amount={}",
                event.getEventType(),
                event.getCustomerId(),
                event.getEmail(),
                event.getOrderId(),
                event.getProduct(),
                event.getQuantity(),
                event.getAmount());

        if (!StringUtils.hasText(event.getEmail())) {
            // Thieu email la loi du lieu -> retry vo ich, de error handler day thang vao DLT
            throw new IllegalArgumentException("Missing email for orderId=" + event.getOrderId());
        }

        String subject;
        String intro;
        switch (event.getEventType()) {
            case ORDER_CREATED -> {
                subject = "Order received " + event.getOrderId();
                intro = "Your order has been received:";
            }
            case ORDER_PAID -> {
                subject = "Payment confirmed " + event.getOrderId();
                intro = "We have received your payment for:";
            }
            case ORDER_CANCELLED -> {
                subject = "Order cancelled " + event.getOrderId();
                intro = "Your order has been cancelled:";
            }
            default -> {
                log.warn("[notify] no template for eventType={}, skipping", event.getEventType());
                return;
            }
        }

        String body = String.format(
                "Hello %s,%n%n" +
                "%s%n" +
                "  - Order ID: %s%n" +
                "  - Product: %s x%d%n" +
                "  - Total amount: %s%n" +
                "  - Time: %s%n%n" +
                "Thank you!",
                event.getCustomerId(),
                intro,
                event.getOrderId(),
                event.getProduct(),
                event.getQuantity(),
                event.getAmount(),
                event.getOccurredAt());

        if (!mailEnabled) {
            log.info("[notify] mail DISABLED (app.mail.enabled=false). Simulating email to {} | subject={}\n{}",
                    event.getEmail(), subject, body);
            return;
        }

        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(from);
        message.setTo(event.getEmail());
        message.setSubject(subject);
        message.setText(body);
        // Khong catch MailException: de loi noi len cho error handler retry, het lan thu thi vao DLT
        mailSender.send(message);
        log.info("[notify] sent email to {} eventType={} orderId={}",
                event.getEmail(), event.getEventType(), event.getOrderId());
    }
}
