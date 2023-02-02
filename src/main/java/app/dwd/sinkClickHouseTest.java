package app.dwd;

import app.Util.MyKafkaUtil;
import app.bean.Project;
import app.sink.ClickHouseUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class sinkClickHouseTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "project";
        String groupId = "ods_project";


        DataStreamSource<String> projectStreamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        SingleOutputStreamOperator<JSONObject> afterStream = projectStreamSource.map(row -> JSON.parseObject(row))
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject data = value.getJSONObject("after");

                        out.collect(data);
                    }
                });

        SingleOutputStreamOperator<Project> ProjectMap = afterStream.map(new MapFunction<JSONObject, Project>() {
            @Override
            public Project map(JSONObject value) throws Exception {
                return new Project(value.getString("id"),
                        value.getString("project_num"),
                        value.getString("project_name"),
                        value.getString("status"),
                        value.getString("client_num"),
                        value.getString("client_name")
                );
            }
        });

        ProjectMap.print("ProjectMap");

        ProjectMap.addSink(ClickHouseUtil.getSink("insert into default.project (id,project_num,project_name,status,client_num,client_name) values (?,?,?,?,?,?)"));


        env.execute();


    }

}