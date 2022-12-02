package com.example.kstream.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReactiveKafkaConsumer {

    private final ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate;


    @EventListener(ApplicationStartedEvent.class)
    public Disposable consume() {
        return reactiveKafkaConsumerTemplate
                .receive()
                .doOnError(error -> log.error("Error receiving event", error))
                .flatMapSequential(this::processRecord)
                .doOnError(error -> log.error("error while reading event", error))
                .subscribe(receiverRecord -> receiverRecord.receiverOffset().acknowledge());
    }

    public Mono<ReceiverRecord<String, String>> processRecord(ReceiverRecord<String, String> receiverRecord) {
        return Mono.just(receiverRecord)
                .doOnNext(record -> log.info("process data here add map/ flatmap and return receiverRecord at the end for ack"))
                .doOnNext(record -> log.info("reactive message received {} and key is {}", record.value(), record.key()));
    }
}
