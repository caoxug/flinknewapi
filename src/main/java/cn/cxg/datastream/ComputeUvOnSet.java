package cn.cxg.datastream;

import cn.cxg.entity.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;

/**
 * @author caoxuguang
 * @Description:
 * @date 2021/11/17 4:06 下午
 */

/*
        -- 窗口1511650000
        543461,1715,1464116,pv,1511658000
        -- 窗口1511660000
        543462,1715,1464116,pv,1511660001
        543463,1715,1464116,pv,1511662000
        --
        543463,1715,1464116,pv,1511663000
        543463,1715,1464116,pv,1511664003
        543464,1715,1464116,pv,1511665000
        -- 窗口1511670000
        543465,1715,1464116,pv,1511675000
 */
public class ComputeUvOnSet {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");
        prop.setProperty("group.id","cxg");
        FlinkKafkaConsumer<String> fkc = new FlinkKafkaConsumer<String>("test01",new SimpleStringSchema(),prop);
        DataStreamSource<String> inputStream = env.addSource(fkc);
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getDataTimestamp()));
        SingleOutputStreamOperator<UserBehavior> pvData = dataStream.filter(data -> "pv".equals(data.getBehavior()));
        SingleOutputStreamOperator<Integer> uv = pvData
                .timeWindowAll(Time.seconds(10))
                .aggregate(new UvCountResult());
        uv.print();
        env.execute("ComputeUvOnSet");
    }
    public static class UvCountResult implements AggregateFunction<UserBehavior, HashSet<Long>,Integer> {

        @Override
        public HashSet<Long> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<Long> add(UserBehavior value, HashSet<Long> accumulator) {
            accumulator.add(value.getUserId());
            return accumulator;
        }

        @Override
        public Integer getResult(HashSet<Long> accumulator) {
            return accumulator.size();
        }

        @Override
        public HashSet<Long> merge(HashSet<Long> a, HashSet<Long> b) {
            a.addAll(b);
            return a;
        }
    }
}
