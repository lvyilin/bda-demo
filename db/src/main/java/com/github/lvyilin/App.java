package com.github.lvyilin;//package com.github.lvyilin;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SparkSession;
//
//import static org.apache.spark.sql.functions.col;
//
//import java.sql.*;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.Properties;
//
//public class App {
//    private static String appName = "spark.sql.demo";
//    private static String master = "local[*]";
//    private static final String USER = "root";
//    private static final String PASSWORD ="123456";
//    private static final String JDBC_URL = "jdbc:mysql://root@192.168.56.122:3306/bigdataDemo?user=root&password=Pass&useUnicode=true&characterEncoding=UTF-8";
//
//
//    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
//
//    public static void main(String[] args) {
////        SparkSession spark = SparkSession
////                .builder()
////                .appName(appName)
////                .master(master)
////                .getOrCreate();
////
////        Dataset<Row> df = spark.read()
////                .format("jdbc")
////                .option("url", "jdbc:mysql://root@cluster2:3306/Persons")
////                .option("dbtable", "Persons")
////                .option("user", "root")
////                .option("password", "123456")
////                .load();
////
////        df.printSchema();
//        readMySQL();
//    }
//
//    public static Connection getConnection() {
//        try {
//            Class.forName(DRIVER_CLASS);
//            return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
//    public static void execute(String sql) throws Exception {
//        Connection conn = null;
//        try {
//            conn = App.getConnection();
//            conn.createStatement().execute(sql);
//            if (!conn.getAutoCommit())
//                conn.commit();
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            if (null != conn) {
//                try {
//                    conn.close();
//                } catch (SQLException e1) {
//                    e1.printStackTrace();
//                }
//            }
//        }
//    }
//
//    public static ResultSet query(String sql) throws Exception {
//        return App.getConnection().createStatement().executeQuery(sql);
//    }
//
//
//    public static void release(ResultSet rs) throws Exception {
//        if (rs != null) {
//            rs.getStatement().getConnection().close();
//            rs.close();
//            rs = null;
//        }
//    }
//
//    private static void readMySQL() {
//        Connection conn = null;
//        Statement stmt = null;
//        try {
//            conn = getConnection();
//            stmt = conn.createStatement();
//            String sql;
//            sql = "SELECT * FROM Persons";
//            ResultSet rs = stmt.executeQuery(sql);
//
//            // 展开结果集数据库
//            while (rs.next()) {
//                // 通过字段检索
//                //PersonID | LastName | FirstName | Address | City
//                int PersonID = rs.getInt("PersonID");
//                String LastName = rs.getString("LastName");
//                String FirstName = rs.getString("FirstName");
//                String Address = rs.getString("Address");
//                String City = rs.getString("City");
//
//                // 输出数据
//                System.out.print("PersonID: " + PersonID);
//                System.out.print(", LastName: " + LastName);
//                System.out.print(", FirstName: " + FirstName);
//                System.out.print(", Address: " + Address);
//                System.out.print(", City: " + City);
//                System.out.print("\n");
//            }
//            // 完成后关闭
//            rs.close();
//            stmt.close();
//            conn.close();
//        } catch (SQLException se) {
//            // 处理 JDBC 错误
//            se.printStackTrace();
//        } catch (Exception e) {
//            // 处理 Class.forName 错误
//            e.printStackTrace();
//        } finally {
//            // 关闭资源
//            try {
//                if (stmt != null) stmt.close();
//            } catch (SQLException se2) {
//            }// 什么都不做
//            try {
//                if (conn != null) conn.close();
//            } catch (SQLException se) {
//                se.printStackTrace();
//            }
//        }
//        System.out.println("Goodbye!");
//    }
//}

import java.sql.*;
import javax.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
public class App
{
    public static void main (String[] args) throws ClassNotFoundException, SQLException {

        String url = "jdbc:mysql://root@cluster2:3306/bigdataDemo";
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, "root", "123456");
        System.out.println("Database connection established");
        conn.close();
        System.out.println("Database connection terminated");

    }
}