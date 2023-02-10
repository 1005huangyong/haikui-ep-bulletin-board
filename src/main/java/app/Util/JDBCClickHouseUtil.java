package app.Util;


import app.common.EPConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JDBCClickHouseUtil extends RichSinkFunction<Row> {
    Connection connection = null;
    String sql;

    public JDBCClickHouseUtil(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String address = "jdbc:clickhouse://" + EPConfig.URL + ":" + EPConfig.HORT + "/" + EPConfig.DATABASE;
        connection = DriverManager.getConnection(address, EPConfig.USERNAME, EPConfig.PASSWORD);
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        PreparedStatement pst = connection.prepareStatement(sql);
        pst.setObject(1, value.getFieldAs(0));
        pst.setObject(2, value.getFieldAs(1));
        pst.setObject(3, value.getFieldAs(2));
        pst.setObject(4, value.getFieldAs(3));
        pst.setObject(5, value.getFieldAs(4));
        pst.setObject(6, value.getFieldAs(5));
        pst.setObject(7, value.getFieldAs(6));
        pst.setObject(8, value.getFieldAs(7));
        pst.execute();
    }

}



