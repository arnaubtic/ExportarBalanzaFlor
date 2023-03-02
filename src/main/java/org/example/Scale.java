package org.example;

import java.sql.*;

public class Scale {
    public static void main(String[] args) {
        Connection();
    }

    //Connect to the mysql database
    public static void Connection() {
        try {
            Connection con = DriverManager.getConnection("jdbc:mysql://10.10.139.191:3306/", "user", "dibal");
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select * from sys_datos.dat_articulo");
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue + " " + rsmd.getColumnName(i));
                }
                System.out.println();
            }
            con.close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

}