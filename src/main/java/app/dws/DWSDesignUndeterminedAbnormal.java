package app.dws;

import app.Util.ClickHouseUtil;
import app.Util.MyKafkaUtil;
import app.bean.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

//设计待定下方异常数
public class DWSDesignUndeterminedAbnormal {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        String topic1 = "project_station";
        String topic2 = "project" ;
        String topic3 = "PLM_LABLE" ;
        String topic4 = "military_draw" ;

        //每个应用单独一个消费者组,不要重名
        String GroupID = "design_8";

        DataStreamSource<String> projectStation = env.addSource(MyKafkaUtil.getKafkaConsumer(topic1, GroupID));

        SingleOutputStreamOperator<ProjectStation> projectStationStream = projectStation.map(JSONObject::parseObject)
                .process(new ProcessFunction<JSONObject, ProjectStation>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<ProjectStation> out) throws Exception {
                        ProjectStation data = value.getObject("after", ProjectStation.class);

                        out.collect(data);
                    }
                });

        DataStreamSource<String> project = env.addSource(MyKafkaUtil.getKafkaConsumer(topic2, GroupID));

        SingleOutputStreamOperator<Project> projectStream = project.map(JSONObject::parseObject)
                .process(new ProcessFunction<JSONObject, Project>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<Project> out) throws Exception {
                        Project data = value.getObject("after", Project.class);

                        out.collect(data);
                    }
                });

        DataStreamSource<String> plmLable = env.addSource(MyKafkaUtil.getKafkaConsumer(topic3, GroupID));

        SingleOutputStreamOperator<PLMLABLE> plmLableStream = plmLable.map(item -> {
            JSONObject jsonObject = JSONObject.parseObject(item);
            PLMLABLE data = jsonObject.getObject("after", PLMLABLE.class);
            data.setPosition_code(data.getProject_num() + "-" + data.getWorkcode());
            return data;
        });

        DataStreamSource<String> militaryDraw = env.addSource(MyKafkaUtil.getKafkaConsumer(topic4, GroupID));

        SingleOutputStreamOperator<MilitaryDraw> militaryDrawStream = militaryDraw.map(item -> {
            JSONObject jsonObject = JSONObject.parseObject(item);
            return jsonObject.getObject("after", MilitaryDraw.class);
        });

        Table project_station = tabEnv.fromDataStream(projectStationStream);
        Table project_tab = tabEnv.fromDataStream(projectStream);
        Table plm_Lable = tabEnv.fromDataStream(plmLableStream);
        Table military_draw = tabEnv.fromDataStream(militaryDrawStream);


        tabEnv.createTemporaryView("ods_project_station", project_station);
        tabEnv.createTemporaryView("ods_project", project_tab);
        tabEnv.createTemporaryView("ods_plm_Lable", plm_Lable);
        tabEnv.createTemporaryView("ods_military_draw", military_draw);

        Table tableResult = tabEnv.sqlQuery("select " +
                "a.id , " +
                "d.project_num, " +
                "a.station_num, " +
                "a.station_name, " +
                "a.drawings as plan_drawings , " +
                "a.real_drawings as actual_numbers , " +
                "from_unixtime((c.mc_plan_accept_time /1000) - 3600*8 ,'yyyy-MM-dd HH:mm:ss' ) as plan_draw_time, " +
                "from_unixtime((b.send_drawing_time /1000) - 3600*8  ,'yyyy-MM-dd HH:mm:ss'  ) as actual_draw_time  " +
                "from ods_project_station as a " +
                "left join ods_project as d on a.project_id = d.id " +
                "left join (select position_code,project_num,workcode,send_drawing_time from ods_plm_Lable where type = '首次' ) b " +
                "on d.project_num = b.project_num and a.station_num = b.workcode " +
                "left join (select * from ods_military_draw where deleted = 0 ) c " +
                "on concat_ws('-', d.project_num , a.station_num) = c.position_code " );

        DataStream<Tuple2<Boolean, DWSDesignUndeterminedAbnormalBean>> resultDs = tabEnv.toRetractStream(tableResult, DWSDesignUndeterminedAbnormalBean.class);

        //resultDs.print();
        SingleOutputStreamOperator<DWSDesignUndeterminedAbnormalBean> result2 = resultDs.map(item -> item.f1);


        result2.addSink(ClickHouseUtil.getSink("insert into design_undetermined_abnormal values(?,?,?,?,?,?,?,?)"));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
