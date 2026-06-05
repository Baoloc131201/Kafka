# Kafka Learn — order-service + notification-service

Hai Spring Boot app de hoc Kafka thuc te:

```
POST /orders ──> [order-service:8081] ──produce──> Kafka topic: order-events
                                                          │
                                                          └──consume──> [notification-service:8082] ──> log "notify customer..."
```

## Topic & flow

- Topic: `order-events` (3 partitions, replication 1)
- Key: `customerId` -> moi customer luon vao **cung 1 partition** => giu thu tu su kien cua customer do
- Value: JSON `OrderEvent { orderId, customerId, product, quantity, amount, createdAt }`
- Consumer group: `notification-service`

## 1. Chay ha tang (Kafka + Zookeeper + kafka-ui)

```bash
docker compose up -d zookeeper kafka kafka-ui
```

Mo Kafka UI: http://localhost:8080

## 2. Chay order-service (producer)

```bash
cd order-service
mvn spring-boot:run
```

App lang nghe o `http://localhost:8081`. Khi khoi dong, `KafkaTopicConfig` se tu tao topic `order-events`.

## 3. Chay notification-service (consumer)

Mo terminal khac:

```bash
cd notification-service
mvn spring-boot:run
```

App nay khong expose HTTP, chi `@KafkaListener` lang nghe topic.

## 4. Goi thu

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "C001",
    "product": "iPhone 15",
    "quantity": 1,
    "amount": 25000000
  }'
```

### Quan sat

- Log **order-service**:
  ```
  [producer] sent orderId=... key=C001 topic=order-events partition=2 offset=0
  ```
- Log **notification-service**:
  ```
  [consumer] received key=C001 partition=2 offset=0
  [notify] -> customer=C001 | orderId=... | product=iPhone 15 x1 | amount=25000000
  ```
- Kafka UI -> Topics -> `order-events`: thay message JSON, partition, offset.

## 5. Test cac kich ban Kafka

### a) Partition theo key
Gui nhieu order voi cung `customerId=C001`: tat ca message vao cung partition. Doi sang `C002`, `C003`: vao partition khac.

### b) Consumer group + scale
Mo them 1 terminal nua chay them 1 instance notification-service (doi port):

```bash
cd notification-service
SERVER_PORT=8083 mvn spring-boot:run
```

Vi cung `group-id=notification-service`, Kafka tu **rebalance**: moi instance se nhan 1 phan partition (vd: 2 instance -> moi instance ~1-2 partition trong 3). Gui chum order va xem log -> phan tan ro rang.

### c) Doi consumer group khac -> nhan lai tu dau
Sua tam `spring.kafka.consumer.group-id` thanh `notification-service-2` roi chay lai: do `auto-offset-reset: earliest`, group moi se replay toan bo message cu trong topic.

## Cau truc

```
Kafka_Learn/
├── docker-compose.yml          # kafka + zookeeper + kafka-ui + redis (co san)
├── order-service/              # producer (port 8081)
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/example/orderservice/
│       │   ├── OrderServiceApplication.java
│       │   ├── config/KafkaTopicConfig.java
│       │   ├── controller/OrderController.java
│       │   ├── model/OrderEvent.java
│       │   └── producer/OrderEventProducer.java
│       └── resources/application.yml
└── notification-service/       # consumer (port 8082)
    ├── pom.xml
    └── src/main/
        ├── java/com/example/notificationservice/
        │   ├── NotificationServiceApplication.java
        │   ├── consumer/OrderEventConsumer.java
        │   ├── model/OrderEvent.java
        │   └── service/NotificationService.java
        └── resources/application.yml
```

## Diem ky thuat dang chu y

| Khia canh | Cho |
| --- | --- |
| Producer idempotence | `application.yml` -> `enable.idempotence: true` -> tranh duplicate khi retry |
| Acks | `acks=all` -> doi tat ca replica xac nhan, khong mat data |
| Key chon partition | `kafkaTemplate.send(topic, key, event)` voi `key = customerId` |
| JSON cross-service | Consumer tat `use.type.headers` + dat `default.type` -> 2 service xai 2 package khac nhau van deserialize OK |
| Consumer group | Cung `group-id` -> share partitions; khac `group-id` -> moi group replay doc lap |
