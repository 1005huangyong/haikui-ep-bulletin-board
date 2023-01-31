package app.ods;


import app.common.EPConfig;
import app.func.CustomerDeserialization;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

import static app.Util.MyKafkaUtil.getKafkaProduce;


public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //执行环境配置
        envConfig(env);

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

    //执行环境配置，检查点，失败重启策略等
    private static void envConfig(StreamExecutionEnvironment env){
        //2.2 设置状态后端
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        //测试环境
        env.getCheckpointConfig().setCheckpointStorage("hdfs://ecs-026:8020/ck/ep");
        //正式环境
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck/pgsql2");
        System.setProperty("HADOOP_USER_NAME", "root");

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