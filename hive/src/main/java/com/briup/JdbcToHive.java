package com.briup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class JdbcToHive {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.239.213:10000/hive",
           "kangdaojian", "hive");

        System.out.println(conn);
        Statement statement = conn.createStatement();

        String sql = "create table t_phone_jdbc1(id int,name string,price double) row format delimited fields terminated by ',' stored as textfile";
        statement.execute(sql);
        System.out.println("创建表成功！");
        conn.close();

    }
}
