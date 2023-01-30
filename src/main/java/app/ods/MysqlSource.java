package app.ods;

import app.Util.MyKafkaUtil;
import app.common.EPConfig;
import app.func.CustomerDeserialization;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MysqlSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DebeziumSourceFunction<String> mySqlData = MySqlSource.<String>builder()
                .hostname(EPConfig.mysql_host)
                .port(EPConfig.mysql_port)
                .username(EPConfig.mysql_user)
                .password(EPConfig.mysql_password)
                .databaseList(EPConfig.mysql_database)
                .tableList("easy_project22.project,easy_project22.project_batch")
                .serverTimeZone(EPConfig.mysql_timezone)
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(mySqlData);

        streamSource.print("source>>>>>>>");

        OutputTag<String> projectOutPutTag = new OutputTag<String>("project") {};
        OutputTag<String> batchOutPutTag = new OutputTag<String>("project_batch") {};

        SingleOutputStreamOperator<String> tableNameDS = streamSource.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        }).process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                String tableName = value.getString("tableName");
                if ("project".equals(tableName)) {
                    ctx.output(projectOutPutTag, value.toJSONString());
                } else if ("project_batch".equals(tableName)) {
                    ctx.output(batchOutPutTag, value.toJSONString());
                }
            }
        });

        DataStream<String> projectOutput = tableNameDS.getSideOutput(projectOutPutTag);
        DataStream<String> batchOutput = tableNameDS.getSideOutput(batchOutPutTag);

        //projectOutput.print();
        //batchOutput.print();

        String project = "ods_project";
        String batch = "ods_project_batch";

        projectOutput.addSink(MyKafkaUtil.getKafkaProduce(project));
        batchOutput.addSink(MyKafkaUtil.getKafkaProduce(batch));
        env.execute();

    }
}
