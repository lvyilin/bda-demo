package com.github.lvyilin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

import java.util.Properties;

public class App {
    private static String appName = "spark.sql.demo";
    private static String master = "local[*]";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://cluster2:3306/hive?useUnicode=true&characterEncoding=utf-8")
                .option("dbtable", "Persons")
                .option("user", "root")
                .option("password", "123456")
                .load();

        df.printSchema();
    }

    private static void readMySQL(SQLContext sqlContext) {
        //jdbc.url=jdbc:mysql://localhost:3306/database
        String url = "jdbc:mysql://192.168.56.122:3306/bigdataDemo";
        //查找的表名
        String table = "Persons";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "123456");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取test数据库中的user_test表内容");
        // 读取表中所有数据
        Dataset<Row> jdbcDF = sqlContext.read().jdbc(url, table, connectionProperties).select("*");
        //显示数据
        jdbcDF.show();
    }
}