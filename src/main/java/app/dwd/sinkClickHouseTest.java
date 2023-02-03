package app.dwd;

import app.Util.MyKafkaUtil;
import app.bean.Project;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class sinkClickHouseTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        String topic = "project";
        String groupId = "ods_project";


        DataStreamSource<String> projectStreamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        SingleOutputStreamOperator<Project> afterStream = projectStreamSource.map(row -> JSON.parseObject(row))
                .process(new ProcessFunction<JSONObject, Project>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<Project> out) throws Exception {
                        Project data = value.getObject("after",Project.class);

                        out.collect(data);
                    }
                });



        afterStream.print("ProjectMap");

        afterStream.print();

       // ProjectMap.addSink(ClickHouseUtil.getSink("insert into default.project (id,project_num,project_name,status,client_num,client_name) values (?,?,?,?,?,?)"));


        env.execute();


    }

}