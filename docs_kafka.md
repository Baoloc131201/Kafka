# Hướng dẫn start project Kafka Demo

## Lệnh chạy nhanh (TL;DR)

```bash
# Start Kafka stack (lần đầu hoặc sau khi đã `docker compose down`)
cd /mnt/d/Kafka_Learn && docker compose up -d

# Start lại container đã có sẵn (sau khi reboot máy / docker compose stop)
docker start zookeeper kafka kafka-ui

# Kiểm tra
docker ps --filter "name=kafka" --filter "name=zookeeper"

# Stop (giữ container)
docker compose stop

# Stop + xóa container
docker compose down
```

Chi tiết troubleshoot ở mục 3 và 7.

## 1. Tổng quan

Project Spring Boot 3.3.4 (Java 17) demo Kafka producer + consumer.

- **App port:** `8088`
- **Kafka broker (host):** `localhost:9092`
- **Kafka broker (trong docker network):** `kafka:29092`
- **Zookeeper:** `localhost:2181`
- **Kafka UI:** http://localhost:8080
- **Consumer group:** `demo-group`

## 2. Yêu cầu

- Docker Desktop (đã có)
- Java 17 + Maven, hoặc IntelliJ IDEA để chạy Spring Boot app

## 3. Khởi động Kafka stack

### Lần đầu (chưa có container)

```bash
cd /mnt/d/Kafka_Learn
docker compose up -d
```

Lệnh trên tạo 3 container: `zookeeper`, `kafka`, `kafka-ui`.

### Container đã tồn tại từ lần chạy trước

Nếu `docker compose up -d` báo lỗi `Conflict. The container name "/kafka" is already in use`, có 2 cách:

**Cách A — start lại container cũ (giữ nguyên):**

```bash
docker start zookeeper kafka kafka-ui
```

**Cách B — xóa và tạo mới (docker-compose.yml hiện không mount volume nên không mất data):**

```bash
docker rm -f kafka kafka-ui zookeeper
docker compose up -d
```

### Kiểm tra Kafka đã sẵn sàng

```bash
docker ps --filter "name=kafka" --filter "name=zookeeper"
docker logs --tail 10 kafka
```

Khi log có dòng `Kafka Server started` là OK.

## 4. Chạy Spring Boot app

### Cách 1: IntelliJ IDEA (recommend khi dev trên Windows)

1. Mở folder `D:\Kafka_Learn` trong IntelliJ.
2. Đợi Maven import xong dependencies.
3. Run class `com.example.kafkademo.KafkaDemoApplication` (nút green ▶).
4. Log xuất hiện `Started KafkaDemoApplication in ...` và app lắng nghe trên `:8088`.

### Cách 2: Maven CLI (cần Java 17 + Maven trên host)

```bash
cd /mnt/d/Kafka_Learn
mvn spring-boot:run
```

Hoặc build jar rồi chạy:

```bash
mvn clean package -DskipTests
java -jar target/kafka-demo-0.0.1-SNAPSHOT.jar
```

### Cách 3: Chạy trong Docker (nếu không có Java trên host)

Tạo `Dockerfile`:

```dockerfile
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/kafka-demo-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8088
ENTRYPOINT ["java","-jar","app.jar"]
```

Thêm service vào `docker-compose.yml` (chú ý dùng `kafka:29092` thay vì `localhost:9092` vì chạy chung network):

```yaml
  app:
    build: .
    container_name: kafka-demo-app
    depends_on:
      - kafka
    ports:
      - "8088:8088"
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
```

Rồi:

```bash
docker compose up -d --build app
```

## 5. Test producer + consumer

### Endpoint plain text

```bash
curl -X POST "http://localhost:8088/api/messages/plain?message=hello"
```

Phản hồi: `Sent: hello`

### Endpoint JSON (MessageDto)

```bash
curl -X POST http://localhost:8088/api/messages \
  -H "Content-Type: application/json" \
  -d '{"content":"hello kafka","sender":"ryan"}'
```

App tự sinh `id` (UUID) nếu không truyền vào.

### Xem consumer nhận message

Mở log của Spring Boot app — `KafkaConsumerService` sẽ in ra message vừa publish (logger level DEBUG đã bật cho `com.example.kafkademo`).

### Xem qua Kafka UI

Mở http://localhost:8080 → cluster `local` → tab **Topics** → chọn topic → tab **Messages**.

## 6. Stop / cleanup

```bash
# Stop app: dừng từ IntelliJ hoặc Ctrl-C terminal đang chạy mvn

# Stop Kafka stack (giữ container)
docker compose stop

# Stop + xóa container
docker compose down
```

## 7. Troubleshoot

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Kafka chưa lên hoặc port 9092 bị chiếm | `docker ps`, `docker logs kafka`; `ss -tlnp \| grep 9092` |
| `Conflict. The container name "/kafka" is already in use` | Container cũ vẫn tồn tại | Xem mục 3 — `docker start ...` hoặc `docker rm -f ...` |
| Consumer không nhận message | `group-id` cũ đã consume rồi nên offset đã chạy hết | Đổi `spring.kafka.consumer.group-id` trong `application.yml`, hoặc xóa group: `docker exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --delete --group demo-group` |
| Lỗi deserialize JSON | Type khác với `MessageDto` | Kiểm tra `spring.json.trusted.packages` và `spring.json.value.default.type` trong `application.yml` |
| Port 8088 đã bị chiếm | App khác đang chạy | Đổi `server.port` trong `application.yml` |
