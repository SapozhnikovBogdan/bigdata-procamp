package com.sapozhnikov.bitcoins.kafka.consumer;


import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sapozhnikov.bitcoins.kafka.consumer.model.BitCoinEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class Main {

    final private static int TOP_COUNT = 10;
    final private static  Comparator<BitCoinEvent> bitCoinPriceDescComparator = Comparator.comparingDouble(BitCoinEvent::getPrice).reversed();

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(Arrays.asList("bitcoin-transactions"));
        List<BitCoinEvent> events = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                if (record.value().isEmpty()) continue;
                events.add(getBitCoinEvent(record.value()));
                //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            if (events.size() <=10) continue;
            events = getTop10(events);
            events.forEach(event -> System.out.println(event));
            System.out.println("------------------------------------------------------------------------------------");
            consumer.commitSync();
        }
    }
    private static KafkaConsumer<String, String> getConsumer(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("enable.auto.commit", "false");
        //props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "100");
        // props.setProperty("max.poll.interval.ms", "4000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    private static List<BitCoinEvent> getTop10(List<BitCoinEvent> eventList){
        if (eventList.isEmpty()) return eventList;

        return eventList.stream()
                .sorted(bitCoinPriceDescComparator)
                .limit(TOP_COUNT)
                .collect(Collectors.toList());
    }

    private static BitCoinEvent getBitCoinEvent(String value){
        ObjectMapper mapper = new ObjectMapper();
        BitCoinEvent bitCoinEvent = null;
        try {
            // read from value, convert it to BitCoinEvent class
            bitCoinEvent = mapper.readValue(value, BitCoinEvent.class);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  bitCoinEvent;
    }
}
