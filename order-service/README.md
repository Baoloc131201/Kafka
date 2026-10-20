# Kafka Learn — order-service + notification-service

Hai Spring Boot app de hoc Kafka theo huong thuc te:

```
POST /orders, /orders/{id}/pay, /orders/{id}/cancel
        │
        ▼
[order-service:8081] ──produce──> topic: orders.events (3 partitions, key = orderId)
                                         │
                                         └──consume──> [notification-service:8082] ──> gui email
                                                         │  loi tam thoi -> retry 3 lan (cach 2s)
                                                         │  loi du lieu / het retry
                                                         ▼
                                       topic: orders.events.notification-service.DLT
```

## Topic & flow

| Topic | Owner | Muc dich |
| --- | --- | --- |
| `orders.events` | order-service | Moi thay doi trang thai don hang la 1 event. Service nao can thi tu subscribe voi group rieng |
| `orders.events.notification-service.DLT` | notification-service | Message notification-service xu ly that bai, cho nguoi kiem tra / replay |

- Key: `orderId` -> moi event cua cung 1 don vao **cung 1 partition** => consumer nhan dung thu tu CREATED -> PAID/CANCELLED
- Value: JSON `OrderEvent { eventId, eventType, occurredAt, orderId, customerId, email, product, quantity, amount }`
- `eventType`: `ORDER_CREATED`, `ORDER_PAID`, `ORDER_CANCELLED`
- Consumer group: `notification-service`

## 1. Chay ha tang (Kafka + kafka-ui)

```bash
docker start zookeeper kafka kafka-ui   # hoac lan dau: docker compose up -d
```

Mo Kafka UI: http://localhost:8080

## 2. Chay order-service (producer)

```bash
cd order-service
mvn spring-boot:run
```

## 3. Chay notification-service (consumer)

```bash
cd notification-service
mvn spring-boot:run                       # mac dinh chi log, khong gui mail that
# Gui mail that qua Gmail (dung App Password, KHONG commit vao code):
MAIL_ENABLED=true MAIL_USERNAME=you@gmail.com MAIL_PASSWORD='app-password' mvn spring-boot:run
```

## 4. Goi thu

```bash
# Tao don -> ORDER_CREATED
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" \
  -d '{"customerId":"C001","email":"test@example.com","product":"iPhone 15","quantity":1,"amount":25000000}'

# Thanh toan -> ORDER_PAID   (hoac /cancel -> ORDER_CANCELLED)
curl -X POST http://localhost:8081/orders/<orderId>/pay

# Xem trang thai
curl http://localhost:8081/orders/<orderId>
```

Chuyen trang thai sai (vd huy don da PAID) -> 409. Don khong ton tai -> 404. Body sai -> 400.

## 5. Test cac kich ban Kafka

### a) Thu tu theo key
Tao don roi pay: 2 event cung `orderId` luon cung partition, offset tang dan.

### b) Loi du lieu -> vao thang DLT (khong retry)
```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 \
  --topic orders.events --property parse.key=true --property key.separator='|' <<'MSG'
bad-1|khong-phai-json
noemail-1|{"eventId":"e1","eventType":"ORDER_CREATED","orderId":"noemail-1","customerId":"C9"}
MSG
```

### c) Loi tam thoi -> retry 3 lan roi vao DLT
Chay notification-service voi SMTP host sai roi tao don:
```bash
MAIL_ENABLED=true MAIL_HOST=smtp.invalid MAIL_USERNAME=x MAIL_PASSWORD=y mvn spring-boot:run
```
Log se co `[retry] attempt=1..4`, sau do message xuat hien trong DLT.

### d) eventType moi ma consumer chua biet -> bo qua, khong ket
Gui event voi `"eventType":"ORDER_SHIPPED"` -> log `unknown/missing eventType, skipping`.

### e) Xem DLT
Kafka UI -> Topics -> `orders.events.notification-service.DLT` -> Messages (header `kafka_dlt-exception-*` cho biet ly do), hoac:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic orders.events.notification-service.DLT --from-beginning --property print.key=true
```

### f) Consumer group + scale
Chay them 1 instance notification-service (`SERVER_PORT=8083 mvn spring-boot:run`): cung group nen Kafka rebalance, moi instance nhan 1 phan trong 3 partition.

## Diem ky thuat dang chu y

| Khia canh | Cho |
| --- | --- |
| Producer idempotence + `acks=all` | order-service `application.yml` -> khong mat, khong trung khi producer retry |
| Key chon partition | `OrderEventProducer` -> `key = orderId` |
| Contract JSON, khong phu thuoc class Java | Producer tat type header, consumer doc thanh `OrderEvent` cua chinh no, bo qua field/eventType la |
| Retry + DLT | notification-service `KafkaConsumerConfig` -> `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` |
| Secret | Mail credentials chi truyen qua bien moi truong |

## Huong mo rong (thuc te hay lam them)

- **Transactional Outbox**: hien don luu trong bo nho va gui Kafka ngay; thuc te luu DB + bang outbox de khong lech DB va Kafka.
- **Idempotent consumer**: luu `eventId` da xu ly (Redis/DB) de khong gui mail 2 lan khi Kafka giao lai message.
- **Schema Registry** (Avro/JSON Schema) thay cho viec copy `OrderEvent` sang 2 service.
- Tao topic bang ha tang (Terraform/Strimzi), `replicas=3`, `min.insync.replicas=2`, tat `auto.create.topics.enable`.
