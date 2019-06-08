package com.github.lvyilin;

public class KafkaConsts {
    public static final String BOOTSTRAP_SERVERS = "cluster1:9092,cluster2:9092,cluster3:9092"; /*需要根据自身机器修改*/
    public static final String ZOOKEEPER_ADDRESS = "cluster2:9092"; /*需要根据自身机器修改*/
    public static final String TOPIC = "GDS";
    public static final String GROUP_ID = "group1";
}
