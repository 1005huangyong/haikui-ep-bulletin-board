package app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLSource {
    public static void main(String[] args) throws Exception {
//1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 正式环境
        String  table1 = "CREATE TABLE groupedmessage (\n" +
                " db_name STRING METADATA FROM 'database_name' VIRTUAL," +
                " table_name STRING METADATA FROM 'table_name' VIRTUAL," +
                " project_num STRING,\n" +
                " project_name STRING,\n" +
                " id int," +
                " PRIMARY KEY(id) NOT ENFORCED " +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '10.22.1.70',\n" +
                "  'username' = 'sunwei',\n" +
                "  'password' = 'Lyric@sunwei',\n" +
                "  'database-name' = 'easy_project22',\n" +
                "  'table-name' = 'project',\n" +
                "  'debezium.snapshot.mode' = 'never',\n" +
                "  'debezium.slot.name' = 'groupedmessage2',\n" +
                "  'port' = '3306'\n" +
                ")";

        tableEnv.executeSql(table1);

        tableEnv.executeSql("select * from  groupedmessage ").print();
        env.execute();


    }
}
