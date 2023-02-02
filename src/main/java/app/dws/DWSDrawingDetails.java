package app.dws;

import app.Util.MyKafkaUtil;
import app.bean.MilitaryDraw;
import app.bean.PLMLABLE;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 出图明细表
 */
public class DWSDrawingDetails {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        String topic1 = "PLM_LABLE";
        String GroupID1 = "ods_PLM_LABLE";

        String topic2 = "military_draw";
        String GroupID2 = "ods_military_draw_1";

        DataStreamSource<String> plmLableStream = env.addSource(MyKafkaUtil.getKafkaConsumer(topic1, GroupID1));

        SingleOutputStreamOperator<PLMLABLE> plmLableAfterStream = plmLableStream.map(JSONObject::parseObject)
                .process(new ProcessFunction<JSONObject, PLMLABLE>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<PLMLABLE> out) throws Exception {
                        PLMLABLE data = value.getObject("after", PLMLABLE.class);

                        data.setPosition_code(data.getProject_num() + "-" + data.getWorkcode());


                        if (data.getSend_drawing_time() != null && data.getSend_drawing_time().toString().length() > 0) {
                            String Send_drawing_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(data.getSend_drawing_time());
                            Date send_drawing_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(Send_drawing_time);
                            data.setSend_drawing_time(send_drawing_time);
                        }

                        out.collect(data);
                    }
                });


        DataStreamSource<String> militaryDrawStream = env.addSource(MyKafkaUtil.getKafkaConsumer(topic2, GroupID2));

        SingleOutputStreamOperator<MilitaryDraw> MilitaryDrawAfterStream = militaryDrawStream.map(JSONObject::parseObject)
                .process(new ProcessFunction<JSONObject, MilitaryDraw>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<MilitaryDraw> out) throws Exception {

                        MilitaryDraw data = value.getObject("after", MilitaryDraw.class);


                        if (data.getMc_plan_accept_time() != null && data.getMc_plan_accept_time().toString().length() > 0) {
                            String Mc_plan_accept_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(data.getMc_plan_accept_time());
                            Date mc_plan_accept_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(Mc_plan_accept_time);
                            data.setMc_plan_accept_time(mc_plan_accept_time);

                        }
                        out.collect(data);
                    }
                });


        tabEnv.createTemporaryView("ods_plm_lable", plmLableAfterStream);
        tabEnv.createTemporaryView("ods_military_draw", MilitaryDrawAfterStream);


        TableResult tableResult = tabEnv.executeSql("select \n" +
                "a.project_num,\n" +
                "a.position_code,\n" +
                "a.mission,\n" +
                "a.plan_number,\n" +
                "a.actual_number,\n" +
                "a.mc_plan_accept_time,\n" +
                "b.send_drawing_time\n" +
                "from ods_military_draw as a\n" +
                "left join (select project_num,position_code,send_drawing_time from ods_plm_lable where type='首次') as b \n" +
                "on a.position_code=b.position_code");


//        TableResult tableResult = tabEnv.executeSql("select mc_plan_accept_time from ods_military_draw");
        tableResult.print();

        env.execute();
    }
}