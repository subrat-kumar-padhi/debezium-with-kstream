package com.example.kstream.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@Configuration
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "consumer-topic-1", containerFactory = "kafkaListenerContainerFactory")
    public void consumeEvents(@Payload String payload,
                              @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                              @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long createdTimestamp,
                              Acknowledgment acknowledgment) {

        log.info("key in final kafka topic: {}", key);
        log.info("payload in final kafka topic: {}", payload);
        acknowledgment.acknowledge();

    }

}
