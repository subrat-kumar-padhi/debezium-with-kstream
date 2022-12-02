package com.example.kstream.producer;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveKafkaProducer {

    private final ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate;


    public Mono<String> publishEvent(String key, String payload, String eventType, String topicName) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                key, payload);
        producerRecord.headers().add("booking_type", eventType.getBytes());

        return reactiveKafkaProducerTemplate.send(producerRecord)
                .doOnNext(result -> log.info("Offset : " + result.recordMetadata().offset() + ", Partition : " + result.recordMetadata().partition()))
                .doOnSuccess(result -> log.info("do something upon successfully producing message"))
                .thenReturn("Success")
                .doOnError(error -> log.error("error while sending data", error))
                .onErrorReturn("ERROR");
    }
}
