package app.ods;


import app.common.EPConfig;
import app.func.CustomerDeserialization;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

import static app.Util.MyKafkaUtil.getKafkaProduce;


public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //表名定义
        String tableNames = "easy_project22.project,easy_project22.project_plan";

        //定义侧输出流标签，并将侧输出流放入HashMap中方便后续调用
        HashMap<String, OutputTag<String>> outMap = new HashMap<>();
        for (String table : tableNames.split(",")) {
            String[] filterTable = table.split("\\.");
            OutputTag<String> outputTag = new OutputTag<String>(filterTable[1]) {};
            outMap.put(filterTable[1],outputTag);
        }

        DebeziumSourceFunction<String> mySqlData = MySqlSource.<String>builder()
                .hostname(EPConfig.mysql_host)
                .port(EPConfig.mysql_port)
                .username(EPConfig.mysql_user)
                .password(EPConfig.mysql_password)
                .databaseList(EPConfig.mysql_database)
                .tableList(tableNames)
                .serverTimeZone(EPConfig.mysql_timezone)
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();


        DataStreamSource<String> streamSource = env.addSource(mySqlData);

        //streamSource.print();

        //两泛型分别是输入输出类型
        SingleOutputStreamOperator<String> mainStream = streamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String table = jsonObject.getString("tableName");
                ctx.output(outMap.get(table), value);
            }
        });

        for (String table : outMap.keySet()) {
            DataStream<String> kafkaOutStream = mainStream.getSideOutput(outMap.get(table));
            kafkaOutStream.addSink(getKafkaProduce(table));
        }

        env.execute();

    }
}