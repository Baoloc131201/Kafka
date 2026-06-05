package com.example.kafkademo.controller;

import com.example.kafkademo.model.MessageDto;
import com.example.kafkademo.producer.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final KafkaProducerService producerService;

    @PostMapping("/plain")
    public ResponseEntity<String> sendPlain(@RequestParam String message) {
        producerService.sendMessage(message);
        return ResponseEntity.ok("Sent: " + message);
    }

    @PostMapping
    public ResponseEntity<MessageDto> send(@RequestBody MessageDto message) {
        if (message.getId() == null || message.getId().isBlank()) {
            message.setId(UUID.randomUUID().toString());
        }
        producerService.sendMessage(message);
        return ResponseEntity.ok(message);
    }
}
