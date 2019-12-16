package com.cherry.calcite.csv;

import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class CsvTest {

    @Test
    public void testMethod() {
        Properties info = new Properties();
        String configPath = "E:\\git\\calcite-example\\src\\main\\resources\\model.json";

        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            /**
             * 使用DriverManager 建立连接，步骤如下：
             */
            Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + configPath, info);
            CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
            Statement st = connection.createStatement();
            ResultSet resultSet = st.executeQuery("SELECT * FROM depts");// where DEPTNO=20
            if (resultSet.next()) {
                System.out.println(resultSet.getString(1) +
                        "\t" + resultSet.getString(2) );
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
