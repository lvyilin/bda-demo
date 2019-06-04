# bda-demo
A big data analytics demo

### 项目结构
```text
produce/ --生产者程序(Kafka)
consume/ --消费者程序(Storm/Spark streaming/单机消费者)
analyse/ --分析日志数据(HBase/HDFS)
db/      --数据库(MySQL)
web/     --Web(Grafana)

utils/   --通用代码(存放硬编码的URL, 配置等信息)
res/     --存放数据，文档，PPT等资源文件
```

### 数据流
![模块](res/module.png)
