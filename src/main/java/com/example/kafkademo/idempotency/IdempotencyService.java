package com.example.kafkademo.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private static final String KEY_PREFIX = "processed:demo-topic:";
    private static final Duration TTL = Duration.ofDays(7);

    private final StringRedisTemplate redis;

    public boolean alreadyProcessed(String messageId) {
        if (messageId == null || messageId.isBlank()) {
            return false;
        }
        Boolean exists = redis.hasKey(KEY_PREFIX + messageId);
        return Boolean.TRUE.equals(exists);
    }

    public void markProcessed(String messageId) {
        if (messageId == null || messageId.isBlank()) {
            return;
        }
        redis.opsForValue().set(KEY_PREFIX + messageId, "1", TTL);
        log.debug("Marked processed: {}", messageId);
    }
}
