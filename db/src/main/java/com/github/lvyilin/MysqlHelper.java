package com.github.lvyilin;

import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

public class MysqlHelper {
    private static final String USER = MysqlConsts.USER;
    private static final String PASSWORD = MysqlConsts.PASSWORD;
    private static final String JDBC_URL = MysqlConsts.JDBC_URL;
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    private Connection conn;

    public static void main(String[] args) {
//        MysqlHelper.readInfo2();
//        MysqlHelper.readInfo1();
//        MysqlHelper.insertInfo2("20011024","1P","MF", "500","200","0.8");
        new MysqlHelper().insertInfo1("20190428", "9P", "BKU", "20");
    }

    public MysqlHelper() {
        conn = getConnection();
    }

    private Connection getConnection() {
        try {
            Class.forName(DRIVER_CLASS);
            return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private void readInfo1() {
        //FIXME: 应该返回一个存放结果的数据结构
        try {
            assert !conn.isClosed();

            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT * FROM info1";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    while (rs.next()) {
                        // 通过字段检索
                        Timestamp time = rs.getTimestamp("time");
                        String requester = rs.getString("requester");
                        String airport = rs.getString("airport");
                        int rq_num = rs.getInt("rq_num");

                        // FIXME：构造成一个数据结构，而不是打印
                        // 输出数据
                        System.out.print("time: " + fmt.format(time));
                        System.out.print(", requester: " + requester);
                        System.out.print(", airport: " + airport);
                        System.out.print(", rq_num: " + rq_num);
                        System.out.print("\n");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private void readInfo2() {
        //TODO: 类似readInfo1
    }

    public void insertInfo1(String time, String requester, String airport, String rq_num) {
        //insert into info1(time,requester,airport,rq_num) values('20190423120011','1P','BKG', 1020);
        try {
            assert !conn.isClosed();
            System.out.println("insertInfo1");
            String sql = "insert into info1(time,requester,airport,rq_num) values(?, ?, ?, ?);";
            try (PreparedStatement preparedStmt = conn.prepareStatement(sql)) {
                preparedStmt.setString(1, time);
                preparedStmt.setString(2, requester);
                preparedStmt.setString(3, airport);
                preparedStmt.setString(4, rq_num);
                preparedStmt.execute();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertInfo2OnSuccess(String time, String requester, String responder, String rq_s_num) {
        try {
            assert !conn.isClosed();

            String sql = "insert into info2(time,requester, responder, rq_s_num) values(?,?,?,?) " +
                    "ON DUPLICATE KEY update rq_s_num=?";

            PreparedStatement preparedStmt = conn.prepareStatement(sql);
            preparedStmt.setString(1, time);
            preparedStmt.setString(2, requester);
            preparedStmt.setString(3, responder);
            preparedStmt.setString(4, rq_s_num);
            preparedStmt.setString(5, rq_s_num);
            preparedStmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertInfo2OnFail(String time, String requester, String responder, String rq_f_num) {
        try {
            assert !conn.isClosed();

            String sql = "insert into info2(time,requester, responder, rq_f_num) values(?,?,?,?)" +
                    "ON DUPLICATE KEY update rq_f_num=?";

            try (PreparedStatement preparedStmt = conn.prepareStatement(sql)) {
                preparedStmt.setString(1, time);
                preparedStmt.setString(2, requester);
                preparedStmt.setString(3, responder);
                preparedStmt.setString(4, rq_f_num);
                preparedStmt.setString(5, rq_f_num);
                preparedStmt.executeUpdate();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
