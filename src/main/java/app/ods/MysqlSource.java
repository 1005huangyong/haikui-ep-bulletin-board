package app.ods;

import app.common.EPConfig;
import app.func.CustomerDeserialization;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class MysqlSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        envConfig(env);
        //easy_project22.project,BlueIot.tb_inspect,databaseTest.kecheng
        DebeziumSourceFunction<String> mySqlData = MySqlSource.<String>builder()
                .hostname(EPConfig.mysql_host)
                .port(EPConfig.mysql_port)
                .username(EPConfig.mysql_user)
                .password(EPConfig.mysql_password)
                .databaseList("easy_project22")
                .tableList("easy_project22.project")
                .serverTimeZone(EPConfig.mysql_timezone)
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(mySqlData);

        streamSource.print("source>>>>>>>");

        /*OutputTag<String> projectOutPutTag = new OutputTag<String>("project") {};
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
        batchOutput.print();*/

/*        String project = "ods_project";
        String batch = "ods_project_batch";

        projectOutput.addSink(MyKafkaUtil.getKafkaProduce(project));
        batchOutput.addSink(MyKafkaUtil.getKafkaProduce(batch));*/
        env.execute();

    }

    //执行环境配置，检查点，失败重启策略等
    private static void envConfig(StreamExecutionEnvironment env) {
        //2.2 设置状态后端
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        //测试环境
        env.getCheckpointConfig().setCheckpointStorage("hdfs://ecs-026:8020/ck/test/mysql");

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔10秒钟做一次CK
        env.enableCheckpointing(10000L);
        //2.2 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 20000L));
    }
}
