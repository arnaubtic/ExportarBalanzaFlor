package org.example;

import java.sql.*;
public class dbMysql {
    private static final String ip = "10.10.139.191";
    private static final String port = "3306";
    private static final String database = "sys_datos";
    private static final String username = "user";
    private static final String password = "dibal";
    private static final String url = "jdbc:mysql://" + ip + ":" + port + "/" + database;

    //Estableix connexió amb ddbb
    public static Connection startConnection() throws SQLException {
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
