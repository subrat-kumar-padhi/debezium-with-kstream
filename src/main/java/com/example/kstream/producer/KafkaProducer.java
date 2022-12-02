package com.example.kstream.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaProducer {

    private static final String SUCCESS = "SUCCESS";
    private static final String BOOKING_EVENT_TYPE = "booking_eventType";
    private static final String AUTHORIZATION = "Authorization";
    private static final String ERROR = "ERROR";

    private final KafkaTemplate<String, String> kafkaTemplate;


    public Mono<String> publishEvent(String payload, String key, String eventType, String token, String topicName) {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                key, payload);
        return Mono.just(producerRecord)
                .doOnNext(rec -> log.info("publishing event with event name as : {} and payload : {} correlationId : {}  topicName : {} ", eventType, payload, key, topicName))
                .map(bytes -> producerRecord.headers().add(BOOKING_EVENT_TYPE, eventType.getBytes()).add(AUTHORIZATION, token.getBytes()))
                .flatMap(headers -> Mono.fromFuture(kafkaTemplate.send(producerRecord).completable()).then(Mono.just(SUCCESS)))
                .doOnError(e -> log.error("Event publishing exception for key {}, event type {} with error message {}", key, eventType, e.getMessage()))
                .onErrorReturn(ERROR);
    }
}
