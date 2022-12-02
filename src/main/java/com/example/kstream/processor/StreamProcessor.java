package com.example.kstream.processor;

import com.example.kstream.models.EventData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.netty.util.internal.StringUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Optional;

/**
 * SOME MUST have pointers:
 * 1. All event must have txid otherwise this code will result in data loss.
 * 2. transaction stream need to be defined last so that when we receive event with END status
 * all the other event are already streamed in kTables.
 * 3.
 **/

@Component
@AllArgsConstructor
@Slf4j
public class StreamProcessor {

    private static final String FINAL_TOPIC = "dbz_7.final_table";
    private static final String CUSTOMER_TOPIC = "dbz_7.C___customer_group";
    private static final String TRANSACTION_TOPIC = "dbz_7.transaction";
    private static final String BOOKING_TOPIC = "dbz_7.C___booking_group";

    private StreamsBuilder streamsBuilder;


    @PostConstruct
    public void streamTopology() {

        var customerTable = streamCustomerTopic().groupByKey()
                .reduce((v1, v2) -> List.of(v1, v2).toString());
        var bookingTable = streamBookingTopic().groupByKey()
                .reduce((v1, v2) -> v1 + "," + v2);

        KTable<String, String> transactionKTable = streamTransactionTopic().toTable(Materialized.as("transaction-table"));

        var finalStream = customerTable.leftJoin(transactionKTable,
                        (v1, v2) -> Optional.ofNullable(v1).orElse(v2))
                .leftJoin(bookingTable, this::transformToDomainEvent);

        finalStream.toStream().groupByKey().reduce((value1, value2) -> value2).toStream()
                .to(FINAL_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }

    private KStream<String, String> streamBookingTopic() {

        KStream<String, String> bookingStream = streamsBuilder.stream(BOOKING_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        bookingStream.peek((k, v) -> log.info("key in booking stream: {} , value in booking stream {} ", k, v));

        return bookingStream.mapValues(v ->
                        EventData.builder().data(v).txId(getValueFromPayload(v, "__txId")).build())
                .selectKey((k, v) -> v.getTxId())
                .mapValues(this::writeObject);

    }

    private KStream<String, String> streamTransactionTopic() {

        KStream<String, String> transactionStream = streamsBuilder.stream(TRANSACTION_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())).filter((k, v) -> getValueFromPayload(v, "status")
                .equalsIgnoreCase("END"));

        transactionStream.peek((k, v) -> log.info("transaction stream key and value is {} and {}", k, v));
        transactionStream.selectKey((k, v) -> getValueFromPayload(v, "id"));
        return transactionStream;
    }

    private KStream<String, String> streamCustomerTopic() {

        KStream<String, String> customerStream = streamsBuilder.stream(CUSTOMER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        customerStream.peek((k, v) -> log.info("key {} and value {} in customer stream:  ", k, v));

        return customerStream.mapValues(v ->
                        EventData.builder().data(v).txId(getValueFromPayload(v, "__txId")).build())
                .selectKey((k, v) -> v.getTxId())
                .mapValues(this::writeObject);
    }

    private String transformToDomainEvent(String value1, String value2) {
        return value1 + "," + value2;
    }

    private String getValueFromPayload(String payload, String identifier) {
        JSONObject jsonObject = new JSONObject(payload);
        return jsonObject.optString(identifier, "");

    }

    private String writeObject(Object obj) {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            return ow.writeValueAsString(obj);
        } catch (Exception e) {
            e.printStackTrace();
            return StringUtil.EMPTY_STRING;
        }

    }


}
