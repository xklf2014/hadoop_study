package com.story.hive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static Connection conn;
    private static Statement stmt;

    @Before
    public void before() throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        conn = DriverManager.getConnection("jdbc:hive2://zknode02:10000/default","root","");
        stmt = conn.createStatement();
    }

    @After
    public void after(){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConn() throws SQLException {



        String sql = "select * from psn limit 5";
        ResultSet rs = stmt.executeQuery(sql);
        while(rs.next()){
            System.out.println(rs.getString(1) + "-" + rs.getString("name"));
        }
    }

    @Test
    public void testSqlArray() throws SQLException {
        String sql = "select hobby[0] from psn limit 5";
        ResultSet rs = stmt.executeQuery(sql);
        while(rs.next()){
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void testSplit() throws SQLException {
        String sql = "select lpad(name,5,'*') from psn2";
        ResultSet rs = stmt.executeQuery(sql);
        while(rs.next()){
            System.out.println(rs.getString(1));
        }
    }
}
