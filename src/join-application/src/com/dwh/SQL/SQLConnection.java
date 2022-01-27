package com.dwh.SQL;

import java.sql.Connection;
import java.sql.DriverManager;

public class SQLConnection {
    String url;
    String user;
    String password;
    Connection con;

    public SQLConnection(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
        try {
            this.con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() {
        try {
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
