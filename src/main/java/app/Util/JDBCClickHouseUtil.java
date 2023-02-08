package app.Util;


import app.common.EPConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JDBCClickHouseUtil extends RichSinkFunction {
    Connection connection = null;
    String sql;

    public JDBCClickHouseUtil(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String  address = "jdbc:clickhouse://" + EPConfig.URL + ":" + EPConfig.HORT + "/" + EPConfig.DATABASE;
        connection = DriverManager.getConnection(address,EPConfig.USERNAME,EPConfig.PASSWORD);
    }

    @Override
    public void close() throws Exception {
        super.close();

        if(connection != null){
            connection.close();
        }
    }

    @Override
    public void invoke(Object value, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
    }


}



