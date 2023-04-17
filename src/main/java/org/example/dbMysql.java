package org.example;

import java.sql.*;

public class dbMysql {
    static String ip;
    static String port;
    static String database;
    static String username;
    static String password;

    //Estableix connexió amb ddbb
    public static Connection startConnection() throws SQLException {
        String url = "jdbc:mysql://" + ip + ":" + port + "/" + database;
        return DriverManager.getConnection(url, username, password);
    }

    //Executa l'statement per fer el select
    public static ResultSet getSelect(Statement stmt, String condicio) throws SQLException {
        return stmt.executeQuery(condicio);
    }

    public static int insert(Statement stmt, String condicio) throws SQLException {
        return stmt.executeUpdate(condicio);
    }

    public static int update(Statement stmt, String condicio) throws SQLException {
        return stmt.executeUpdate(condicio);
    }
}
