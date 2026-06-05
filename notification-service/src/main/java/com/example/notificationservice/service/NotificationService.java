package com.example.notificationservice.service;

import com.example.notificationservice.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
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
        log.info("[notify] -> customer={} | email={} | orderId={} | product={} x{} | amount={} | createdAt={}",
                event.getCustomerId(),
                event.getEmail(),
                event.getOrderId(),
                event.getProduct(),
                event.getQuantity(),
                event.getAmount(),
                event.getCreatedAt());

        sendEmail(event);
    }

    private void sendEmail(OrderEvent event) {
        if (!StringUtils.hasText(event.getEmail())) {
            log.warn("[notify] no email for orderId={}, skipping mail send", event.getOrderId());
            return;
        }

        String subject = "Order confirmation " + event.getOrderId();
        String body = String.format(
                "Hello %s,%n%n" +
                "Your order has been received:%n" +
                "  - Order ID: %s%n" +
                "  - Product: %s x%d%n" +
                "  - Total amount: %s%n" +
                "  - Time: %s%n%n" +
                "Thank you for your purchase!",
                event.getCustomerId(),
                event.getOrderId(),
                event.getProduct(),
                event.getQuantity(),
                event.getAmount(),
                event.getCreatedAt());

        if (!mailEnabled) {
            log.info("[notify] mail DISABLED (app.mail.enabled=false). Simulating email send to {}:\n{}",
                    event.getEmail(), body);
            return;
        }

        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(from);
            message.setTo(event.getEmail());
            message.setSubject(subject);
            message.setText(body);
            mailSender.send(message);
            log.info("[notify] sent email to {} orderId={}", event.getEmail(), event.getOrderId());
        } catch (MailException ex) {
            log.error("[notify] FAILED to send email to {} for orderId={}: {}",
                    event.getEmail(), event.getOrderId(), ex.getMessage(), ex);
        }
    }
}
