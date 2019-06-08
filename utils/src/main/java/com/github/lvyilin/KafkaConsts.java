package com.github.lvyilin;

public class KafkaConsts {
    public static final String BOOTSTRAP_SERVERS = "cluster1:9092,cluster2:9092,cluster3:9092"; /*添加cluster{1,2,3}到本机的hosts（推荐），或者根据自身机器修改成IP*/
    public static final String ZOOKEEPER_ADDRESS = "cluster1:2181,cluster2:2181,cluster3:2181"; /*添加cluster{1,2,3}到本机的hosts（推荐），或者根据自身机器修改成IP*/
    public static final String TOPIC = "GDS";
    public static final String GROUP_ID = "group1";
}
