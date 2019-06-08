package com.github.lvyilin;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws FileNotFoundException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConsts.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        BufferedReader br = new BufferedReader(new FileReader(ConfigConsts.DATA_PATH));

        try (org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(properties)) {
            int i = 1;
            while (true) {
                String msg = br.readLine();
                if (msg == null) break;
                producer.send(new ProducerRecord<String, String>(KafkaConsts.TOPIC, msg));
                System.out.printf("Sent %d: %s\n", i++, msg);
            }
        } catch (Exception e) {
            e.printStackTrace();

        }

    }
}
