package com.example.springbootkafka.producer;

import com.example.springbootkafka.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private static final String TOPIC = "orders";

    // 비동기 방식
    // kafkaTemplate.send(...)는 비동기적으로 메시지를 Kafka에 전송함.
    // 메시지 전송이 완료되면 whenComplete가 실행됨.
    public void sendOrder(OrderEvent order) {
        kafkaTemplate.send(TOPIC, order.getOrderId(), order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message: {}", order.getOrderId(), ex);
                    } else {
                        log.info("Message sent successfully: {}, partition: {}",
                                order.getOrderId(), result.getRecordMetadata().partition());
                    }
                });
    }

    // 동기방식
    // kafkaTemplate.send(...).get(); 을 사용해서 메시지를 보낸 후, 전송이 완료될 때까지 기다림
    public void sendOrderSync(OrderEvent order) throws Exception {
        try {
            // send의 result를 받는다.
            SendResult<String, OrderEvent> result = kafkaTemplate.send(TOPIC, order.getOrderId(), order).get();
            log.info("Message sent synchronously: {}, partition: {}",
                    order.getOrderId(), result.getRecordMetadata().partition());
        } catch (Exception e) {
            log.error("Error sending message synchronously", e);
            throw e;
        }
    }
}
