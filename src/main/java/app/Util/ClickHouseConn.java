package app.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ClickHouseConn {
    private static Connection connection;

    public static Connection getConn(String host, int port, String database,String username,String password) throws SQLException, ClassNotFoundException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String  address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(address,username,password);
        return connection;
    }
    public void close() throws SQLException {
        connection.close();
    }
}