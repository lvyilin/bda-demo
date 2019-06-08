package com.github.lvyilin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster2:9092");
        props.put("zookeeper.connect", KafkaConsts.ZOOKEEPER_ADDRESS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsts.GROUP_ID);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS, "4000");
//        props.put("zookeeper.sync.time.ms", "200");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "range");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(KafkaConsts.TOPIC);
            while (true) {
                Map<String, ConsumerRecords<String, String>> records = consumer.poll(10000);
                process(records);
                Thread.sleep(1000);
            }
        }
    }

    private static void process(Map<String, ConsumerRecords<String, String>> records) throws Exception {
        if (records == null) return;
        for (Map.Entry<String, ConsumerRecords<String, String>> recordMetadata : records.entrySet()) {
            List<ConsumerRecord<String, String>> recordsPerTopic = recordMetadata.getValue().records();
            for (ConsumerRecord<String, String> record : recordsPerTopic) {
                // process record
                System.out.println(record.value());

            }
        }
    }

}