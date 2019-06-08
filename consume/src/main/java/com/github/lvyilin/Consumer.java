package com.github.lvyilin;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Consumer {

    private static final int THREADS_NUM = 1;
    private static final String CLIENT_ID = "consumer_test_id";
    private final ConsumerConnector consumerConnector;
    private ExecutorService executor;

    public Consumer() {
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(buildProperties());
        executor = Executors.newFixedThreadPool(THREADS_NUM);
    }


    private ConsumerConfig buildProperties() {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaConsts.ZOOKEEPER_ADDRESS);
        props.put("group.id", KafkaConsts.GROUP_ID);

        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        props.put("client.id", CLIENT_ID);
        return new ConsumerConfig(props);
    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(KafkaConsts.TOPIC, THREADS_NUM);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(KafkaConsts.TOPIC);

        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerExecutorService(stream));
        }
    }


    public static void main(String[] args) {
//        BasicConfigurator.configure();

        new Consumer().run();
    }
}