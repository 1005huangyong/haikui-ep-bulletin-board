package app.Util;

import app.bean.ProjectStation;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 数据处理工具类
 * 封装通用的处理方法
 */
public class DataDealUtil<T> {

    //ods数据包装为Bean格式
    public  SingleOutputStreamOperator<T> odsDataResolving(String topic, String group, StreamExecutionEnvironment env,Class<T> tClass){
        DataStreamSource<String> resultDs = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, group));

        return resultDs.map(JSONObject::parseObject)
                .process(new ProcessFunction<JSONObject, T>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<T> out) throws Exception {
                        T data = value.getObject("after", tClass);
                        out.collect(data);
                    }
                });
    }
}
