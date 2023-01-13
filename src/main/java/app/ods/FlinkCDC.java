package app.ods;


import app.common.EPConfig;
import app.func.CustomerDeserialization;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置debezium参数
        Properties properties = new Properties();
        properties.put("scan.incremental.snapshot.enabled","true");

        DebeziumSourceFunction<String> mySqlData = MySqlSource.<String>builder()
                .hostname(EPConfig.mysql_host)
                .port(EPConfig.mysql_port)
                .username(EPConfig.mysql_user)
                .password(EPConfig.mysql_password)
                .databaseList(EPConfig.mysql_database)
                .tableList("easy_project22.project")
                .serverTimeZone(EPConfig.mysql_timezone)
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .build();



        DataStreamSource<String> streamSource = env.addSource(mySqlData);

        streamSource.print();



        env.execute();

    }
}