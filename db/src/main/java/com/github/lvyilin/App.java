package com.github.lvyilin;

import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class App {
    private static final String USER = MysqlConsts.USER;
    private static final String PASSWORD = MysqlConsts.PASSWORD;
    private static final String JDBC_URL = MysqlConsts.JDBC_URL;

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    public static void main(String[] args) {
//        App.readInfo2();
//        App.readInfo1();
//        App.insertInfo2("20011024","1P","MF", "500","200","0.8");
//        App.insertInfo1("20190428","9P","BKU", "20");
    }

    private static Connection getConnection() {
        try {
            Class.forName(DRIVER_CLASS);
            return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    private static void execute(String sql) {
        try {
            stmt.execute(sql);
            if (!conn.getAutoCommit())
                conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ResultSet query(String sql) throws Exception {
        return stmt.executeQuery(sql);
    }


    private static void release() throws Exception {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }


    private static void readInfo1() {

        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT * FROM info1";
            rs = query(sql);
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while (rs.next()) {
                // 通过字段检索
                Timestamp time = rs.getTimestamp("time");
                String requester = rs.getString("requester");
                String airport = rs.getString("airport");
                int rq_num = rs.getInt("rq_num");
                // 输出数据
                System.out.print("time: " + fmt.format(time));
                System.out.print(", requester: " + requester);
                System.out.print(", airport: " + airport);
                System.out.print(", rq_num: " + rq_num);
                System.out.print("\n");
            }
            // 完成后关闭
            release();
        } catch (Exception se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        }
    }


    private static void readInfo2() {
        //info2(time,requester, responder, rq_s_num, rq_f_num,rate)
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT * FROM info2";
            rs = query(sql);
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while (rs.next()) {
                // 通过字段检索


                Timestamp time = rs.getTimestamp("time");
                String requester = rs.getString("requester");
                String responder = rs.getString("responder");
                int rq_s_num = rs.getInt("rq_s_num");
                int rq_f_num = rs.getInt("rq_f_num");
                double rate = rs.getDouble("rate");


                // 输出数据
                System.out.print("time: " + fmt.format(time));
                System.out.print(", requester: " + requester);
                System.out.print(", responder: " + responder);
                System.out.print(", rq_s_num: " + rq_s_num);
                System.out.print(", rq_f_num: " + rq_f_num);
                System.out.print(", rate: " + rate);
                System.out.print("\n");
            }
            // 完成后关闭
            release();
        } catch (Exception se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        }
    }

    public static void insertInfo1(String time,String requester,String airport,String rq_num) {
        //insert into info1(time,requester,airport,rq_num) values('20190423120011','1P','BKG', 1020);
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            //drop database if exists db_name;
            String sql = "insert into info1(time,requester,airport,rq_num) values('"+time+"','"+requester+"','"+airport+"',"+rq_num+");";
            execute(sql);
            release();
        } catch (Exception se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        }
    }

    public static void insertInfo2(String time,String requester,String responder,String rq_s_num,String rq_f_num,String rate) {
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            //drop database if exists db_name;
            String sql = "insert into info2(time,requester, responder, rq_s_num, rq_f_num,rate) values('"+time+"','"+requester+"','"+responder+"',"+rq_s_num+","+rq_f_num+","+rate+");";

            execute(sql);

            release();
        } catch (Exception se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        }
    }
}
