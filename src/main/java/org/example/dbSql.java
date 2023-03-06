package org.example;

import java.sql.*;
public class dbSql {
    private static final String ip = "10.10.139.51";
    private static final String port = "64952";
    private static final String database = "MBVic";
    private static final String username = "logic";
    private static final String password = "Sage2009+";
    private static final String url = "jdbc:jtds:sqlserver://" + ip + ":" + port + "/" + database;


    //Estableix connexió amb ddbb
    public static Connection startConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    //Executa l'statement per fer el select
    public static ResultSet getSelect(Statement stmt, String condicio) throws SQLException {
        return stmt.executeQuery(condicio);
    }

    public static void insert(Statement stmt, String condicio) throws SQLException {
        stmt.executeUpdate(condicio);
    }
}
