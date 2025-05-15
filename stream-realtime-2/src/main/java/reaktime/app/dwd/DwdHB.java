package reaktime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.cm.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Package reaktime.app.dwd.DwdHB
 * @Author chen.ming
 * @Date 2025/5/15 21:58
 * @description:
 */
public class DwdHB {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_order_info_base_chenming", "dwd_log");

        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        SingleOutputStreamOperator<JSONObject> map = kafkaStrDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = new JSONObject();
                jsonObject.getJSONObject(value);
                return jsonObject;
            }
        });


        SingleOutputStreamOperator<JSONObject> tsMs = map.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts_ms");
            }
        }));

        tsMs.print();

        env.execute();

    }
}
