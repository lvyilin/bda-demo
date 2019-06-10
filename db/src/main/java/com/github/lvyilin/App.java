package com.github.lvyilin;

import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class App {
    private static final String USER = "root";
    private static final String PASSWORD ="123456";
    private static final String JDBC_URL = "jdbc:mysql://cluster2:3306/bigdataDemo";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    public static void main(String[] args) {
        App.readMySQL();
    }

    private static Connection getConnection() {
        try {
            Class.forName(DRIVER_CLASS);
            return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    private static void execute(String sql){
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
        if (rs!= null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    private static void readMySQL() {

        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT * FROM Persons";
            String sql1 = "insert into Persons values(2,'哈哈','hua','bjtu','beijing');";
            String sql2 = "delete from Persons where LastName = '??';";
            execute(sql2);
            rs = query(sql);

            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索
                //PersonID | LastName | FirstName | Address | City
                int PersonID = rs.getInt("PersonID");
                String LastName = rs.getString("LastName");
                String FirstName = rs.getString("FirstName");
                String Address = rs.getString("Address");
                String City = rs.getString("City");

                // 输出数据
                System.out.print("PersonID: " + PersonID);
                System.out.print(", LastName: " + LastName);
                System.out.print(", FirstName: " + FirstName);
                System.out.print(", Address: " + Address);
                System.out.print(", City: " + City);
                System.out.print("\n");
            }
            // 完成后关闭
            release();
        } catch (Exception se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        }

        System.out.println("Goodbye!");
    }
}
