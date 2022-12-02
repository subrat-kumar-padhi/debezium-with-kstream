package com.example.kstream.controller;

import com.example.kstream.producer.KafkaProducer;
import com.example.kstream.producer.ReactiveKafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/demo")
@RequiredArgsConstructor
@Slf4j
public class DemoController {

    private final KafkaProducer kafkaProducer;
    private final ReactiveKafkaProducer reactiveKafkaProducer;


    @PostMapping("push/non-reactive")
    public Mono<String> pushNonReactiveMsg() {
        return kafkaProducer.publishEvent("non reactive msg", "key-1", "non-reactive",
                        "token-1", "consumer-topic-1")
                .doOnSuccess(s -> log.info("successfully send msg"));
    }

    @PostMapping("push/reactive")
    public Mono<String> pushReactiveMsg() {
        return reactiveKafkaProducer.publishEvent("reactive msg ", "key-1", "reactive", "consumer-topic-2")
                .doOnSuccess(s -> log.info("Successfully send reactive msg"));
    }
}
