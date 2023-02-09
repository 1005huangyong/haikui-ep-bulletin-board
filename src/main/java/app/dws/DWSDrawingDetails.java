package app.dws;

import app.Util.JDBCClickHouseUtil;
import app.Util.MyKafkaUtil;
import app.bean.MilitaryDraw;
import app.bean.PLMLABLE;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * 出图明细表
 */
public class DWSDrawingDetails {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        String topic1 = "PLM_LABLE";
        String GroupID1 = "ods_PLM_LABLE1";

        String topic2 = "military_draw";
        String GroupID2 = "ods_military_draw";

        DataStreamSource<String> plmLableStream = env.addSource(MyKafkaUtil.getKafkaConsumer(topic1, GroupID1));

        SingleOutputStreamOperator<PLMLABLE> plmLableAfterStream = plmLableStream.map(JSONObject::parseObject)
                .process(new ProcessFunction<JSONObject, PLMLABLE>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<PLMLABLE> out) throws Exception {
                        PLMLABLE data = value.getObject("after", PLMLABLE.class);

                        data.setPosition_code(data.getProject_num() + "-" + data.getWorkcode());


                        out.collect(data);
                    }
                });


        DataStreamSource<String> militaryDrawStream = env.addSource(MyKafkaUtil.getKafkaConsumer(topic2, GroupID2));

        SingleOutputStreamOperator<MilitaryDraw> MilitaryDrawAfterStream = militaryDrawStream.map(JSONObject::parseObject)
                .process(new ProcessFunction<JSONObject, MilitaryDraw>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<MilitaryDraw> out) throws Exception {

                        MilitaryDraw data = value.getObject("after", MilitaryDraw.class);


                        out.collect(data);
                    }
                });


        Table ods_plm_lable_temp = tabEnv.fromDataStream(plmLableAfterStream);
        Table ods_military_draw_temp = tabEnv.fromDataStream(MilitaryDrawAfterStream);

        tabEnv.createTemporaryView("ods_plm_lable", ods_plm_lable_temp);
        tabEnv.createTemporaryView("ods_military_draw", ods_military_draw_temp);


        Table tableResult = tabEnv.sqlQuery("select \n" +
                "a.id,\n" +
                "a.project_num,\n" +
                "a.position_code,\n" +
                "a.mission,\n" +
                "a.plan_number,\n" +
                "a.actual_number,\n" +
                "a.mc_plan_accept_time,\n" +
                "b.send_drawing_time \n" +
                "from ods_military_draw as a \n" +
                "left join (select project_num,position_code,send_drawing_time from ods_plm_lable where type='首次') as b \n" +
                "on a.position_code=b.position_code");


        String sql = "insert into table default.DWSDrawingDetails_1(id,project_num,position_code,mission,plan_number,actual_number,mc_plan_accept_time,send_drawing_time)" +
                " values(?,?,?,?,?,?,?,?)";

        JDBCClickHouseUtil jdbcSink = new JDBCClickHouseUtil(sql);

        DataStream<Row> rowDataStream = tabEnv.toChangelogStream(tableResult);

        DataStream<Row> resultDataStream = tabEnv.toChangelogStream(tableResult,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("project_num", DataTypes.STRING().bridgedTo(StringData.class))
                        .column("position_code", DataTypes.STRING().bridgedTo(StringData.class))
                        .column("mission", DataTypes.STRING().bridgedTo(StringData.class))
                        .column("plan_number", DataTypes.INT())
                        .column("actual_number", DataTypes.INT())
                        .column("mc_plan_accept_time", DataTypes.DATE())
                        .column("send_drawing_time", DataTypes.DATE())
                        .build());



       resultDataStream.addSink(jdbcSink);
       // resultDataStream.print();
        //tabEnv.toChangelogStream(tableResult).print();

        env.execute();
    }

}