package com.github.lvyilin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;


public class Analyser implements Serializable {
    private static final String APP_NAME = "TicketAnalyse";
    private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    private static LocalDateTime MIN_DATETIME = fmt.parseLocalDateTime("20190423000000");
    private static LocalDateTime MAX_DATETIME = fmt.parseLocalDateTime("20190423230000");
    private static SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster(SparkConsts.SPARK_MASTER_URL);
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;

    private void task1() {
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cluster1,cluster2,cluster3");
        String tableName = HBaseConsts.TABLE_NAME;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.addFamily(Bytes.toBytes("ticket"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("req"));
        scan.addColumn(Bytes.toBytes("ticket"), Bytes.toBytes("airport"));

        startDateTime = MIN_DATETIME;
        endDateTime = MIN_DATETIME.plusHours(1);

        do {
            scan.setStartRow(Bytes.toBytes(startDateTime.toString(fmt)));
            scan.setStopRow(Bytes.toBytes(endDateTime.toString(fmt)));
            try {
                ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
                String ScanToString = Base64.encodeBytes(proto.toByteArray());

                conf.set(TableInputFormat.SCAN, ScanToString);
                JavaPairRDD<ImmutableBytesWritable, Result> originalRDD = ctx.newAPIHadoopRDD(conf,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class);

                // transformation & action here
                JavaRDD<String> recordRDD = originalRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String>() {
                    @Override
                    public Iterable<String> call(Tuple2<ImmutableBytesWritable, Result> tuple) {
                        String req = Bytes.toString(tuple._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("req")));
                        String airport = Bytes.toString(tuple._2.getValue(Bytes.toBytes("ticket"), Bytes.toBytes("airport")));
                        return Collections.singletonList(req + "#" + airport);
                    }

                });
                JavaPairRDD<String, Integer> pairRDD = recordRDD.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });

                JavaPairRDD<String, Integer> countRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

                JavaRDD<String> finalRDD = countRDD.map(new Function<Tuple2<String, Integer>, String>() {
                    @Override
                    public String call(Tuple2<String, Integer> tuple) throws Exception {
                        return startDateTime.toString(fmt) + "#" + tuple._1() + "#" + tuple._2();
                    }
                });
                finalRDD.coalesce(1).saveAsTextFile("/res1-" + startDateTime.toString(fmt));

            } catch (IOException e) {
                e.printStackTrace();
            }
            startDateTime = new LocalDateTime(endDateTime);
            endDateTime = endDateTime.plusHours(1);
        } while (!startDateTime.isAfter(MAX_DATETIME));

        ctx.stop();
    }

    private void task2() {
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cluster1,cluster2,cluster3");

        String tableName = HBaseConsts.TABLE_NAME;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("req"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("res"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("success"));

        startDateTime = MIN_DATETIME;
        endDateTime = MIN_DATETIME.plusHours(1);

        do {
            scan.setStartRow(Bytes.toBytes(startDateTime.toString(fmt)));
            scan.setStopRow(Bytes.toBytes(endDateTime.toString(fmt)));
            try {
                ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
                String ScanToString = Base64.encodeBytes(proto.toByteArray());

                conf.set(TableInputFormat.SCAN, ScanToString);
                JavaPairRDD<ImmutableBytesWritable, Result> originalRDD = ctx.newAPIHadoopRDD(conf,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class);

                // transformation & action here
                JavaRDD<String> recordRDD = originalRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String>() {
                    @Override
                    public Iterable<String> call(Tuple2<ImmutableBytesWritable, Result> tuple) {
                        String req = Bytes.toString(tuple._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("req")));
                        String res = Bytes.toString(tuple._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("res")));
                        String success = Bytes.toString(tuple._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("success")));
                        return Collections.singletonList(req + "#" + res + "#" + success);
                    }

                });
                JavaPairRDD<String, Integer> pairRDD = recordRDD.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });

                JavaPairRDD<String, Integer> countRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });
                JavaRDD<String> finalRDD = countRDD.map(new Function<Tuple2<String, Integer>, String>() {
                    @Override
                    public String call(Tuple2<String, Integer> tuple) throws Exception {
                        return startDateTime.toString(fmt) + "#" + tuple._1() + "#" + tuple._2();
                    }
                });
                finalRDD.coalesce(1).saveAsTextFile("/res2-" + startDateTime.toString(fmt));
            } catch (IOException e) {
                e.printStackTrace();
            }
            startDateTime = new LocalDateTime(endDateTime);
            endDateTime = endDateTime.plusHours(1);
        } while (!startDateTime.isAfter(MAX_DATETIME));

        ctx.stop();
    }

    public static void main(String[] args) {
//        new Analyser().task1();
        new Analyser().task2();
    }
}
