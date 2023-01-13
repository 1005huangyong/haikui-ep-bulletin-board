package app.Util;

import app.common.EPConfig;
import app.func.CustomerDeserialization;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;

public class MysqlSourceUtil {
    public static DebeziumSourceFunction<String> getMysqlSource(String tableName) {
        DebeziumSourceFunction<String> mySqlData= MySqlSource.<String>builder()
                .hostname(EPConfig.mysql_host)
                .port(EPConfig.mysql_port)
                .username(EPConfig.mysql_user)
                .password(EPConfig.mysql_password)
                .databaseList(EPConfig.mysql_database)
                .tableList(tableName)
                .serverTimeZone(EPConfig.mysql_timezone)
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();
        return mySqlData;
    }

}