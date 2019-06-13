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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Analyser implements Serializable {
    private static final String APP_NAME = "TicketAnalyse";
    private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    private static LocalDateTime MIN_DATETIME = fmt.parseLocalDateTime("20190423000000");
    private static LocalDateTime MAX_DATETIME = fmt.parseLocalDateTime("20190423230000");
    private static SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster(SparkConsts.SPARK_MASTER_URL);
    private static ArrayList<String> info1List = new ArrayList<>();
    private static ArrayList<String> info2List = new ArrayList<>();

    private void task1() {
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cluster1,cluster2,cluster3");

        String tableName = HBaseConsts.TABLE_NAME;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
//        String columns = "info:req ticket:airport";
//        conf.set(TableInputFormat.SCAN_COLUMNS, columns);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.addFamily(Bytes.toBytes("ticket"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("req"));
        scan.addColumn(Bytes.toBytes("ticket"), Bytes.toBytes("airport"));

        LocalDateTime startDateTime = MIN_DATETIME;
        LocalDateTime endDateTime = MIN_DATETIME.plusHours(1);

//        MysqlHelper dbHelper = new MysqlHelper();
        do {
//            System.out.println(startDateTime.toString(fmt));
//            System.out.println(endDateTime.toString(fmt));

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
                List<Tuple2<String, Integer>> output = countRDD.collect();
                for (Tuple2<?, ?> tuple : output) {
                    System.out.println(startDateTime.toString(fmt) + " : " + tuple._1() + " : " + tuple._2());
                    // TODO: save to mysql

                    String[] splitInfo = tuple._1().toString().split("#");
                    assert splitInfo.length == 2;
                    info1List.add(startDateTime.toString(fmt) + "#" + splitInfo[0] + "#" + splitInfo[1] + "#" + tuple._2());
//                    dbHelper.insertInfo1(startDateTime.toString(fmt), splitInfo[0], splitInfo[1], (String) tuple._2());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            startDateTime = endDateTime;
            endDateTime = endDateTime.plusHours(1);
        } while (!startDateTime.isAfter(MAX_DATETIME));

//        dbHelper.close();
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

        LocalDateTime startDateTime = MIN_DATETIME;
        LocalDateTime endDateTime = MIN_DATETIME.plusHours(1);

//        MysqlHelper dbHelper = new MysqlHelper();
        do {
//            System.out.println(startDateTime.toString(fmt));
//            System.out.println(endDateTime.toString(fmt));

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
                List<Tuple2<String, Integer>> output = countRDD.collect();
                for (Tuple2<?, ?> tuple : output) {
                    System.out.println(startDateTime.toString(fmt) + " : " + tuple._1() + " : " + tuple._2());
                    // TODO: save to mysql

                    String[] splitInfo = tuple._1().toString().split("#");
                    assert splitInfo.length == 3;
                    boolean isSuccess = splitInfo[2].equals("1");
                    info2List.add(isSuccess + "#" + startDateTime.toString(fmt) + "#" + splitInfo[0] + "#" + splitInfo[1] + "#" + tuple._2());
//                    if (isSuccess) {
//                        dbHelper.insertInfo2OnSuccess(startDateTime.toString(fmt), splitInfo[0], splitInfo[1], (String) tuple._2());
//                    } else {
//                        dbHelper.insertInfo2OnFail(startDateTime.toString(fmt), splitInfo[0], splitInfo[1], (String) tuple._2());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            startDateTime = endDateTime;
            endDateTime = endDateTime.plusHours(1);
        } while (!startDateTime.isAfter(MAX_DATETIME));

//        dbHelper.close();
        ctx.stop();
    }

    public static void main(String[] args) {
        new Analyser().task1();
        new Analyser().task2();
        MysqlHelper dbHelper = new MysqlHelper();
        System.out.println("db connected!");
        for (String str : info1List) {
            String[] splitInfo = str.split("#");
            dbHelper.insertInfo1(splitInfo[0], splitInfo[1], splitInfo[2], splitInfo[3]);
        }
        for (String str : info2List) {
            String[] splitInfo = str.split("#");
            boolean isSuccess = splitInfo[0].equals("1");
            if (isSuccess) {
                dbHelper.insertInfo2OnSuccess(splitInfo[0], splitInfo[1], splitInfo[2], splitInfo[3]);
            } else {
                dbHelper.insertInfo2OnFail(splitInfo[0], splitInfo[1], splitInfo[2], splitInfo[3]);
            }
        }
        dbHelper.close();
    }
}
