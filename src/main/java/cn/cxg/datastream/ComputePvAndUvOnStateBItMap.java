package cn.cxg.datastream;
import cn.cxg.entity.UserBehavior;
import cn.cxg.entity.UserClickModel;
import cn.cxg.sink.MyMysqlSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.streaming.api.windowing.time.Time.*;

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
        543463,1715,1464116,pv,1511663000
        --
        543463,1715,1464116,pv,1511663001
        543463,1715,1464116,pv,1511664003
        543464,1715,1464116,pv,1511665000
        -- 窗口1511670000
        543465,1715,1464116,pv,1511675000
 */
public class ComputePvAndUvOnStateBItMap {
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
        SingleOutputStreamOperator<UserClickModel> uv = pvData
                .keyBy(data -> data.getCategoryId())
                // 一天为窗口，指定时间起点比时间戳时间早8个小时
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                // 10s触发一次一天数据的计算，更新统计结果
                .trigger(ContinuousEventTimeTrigger.of(seconds(10)))
                .process(new MyProcessWindowFunctionBitMap());
//        uv.print();
        //写入mysql
       uv.addSink(new MyMysqlSink());
        env.execute("ComputePvAndUvOnState");
    }
    public static class MyProcessWindowFunctionBitMap extends ProcessWindowFunction<UserBehavior, UserClickModel,Integer, TimeWindow> {
        private transient ValueState<Integer> pvState;
        private transient ValueState<Roaring64NavigableMap> uvState;
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> pvStateDescriptor = new ValueStateDescriptor<>("pv", Integer.class);
            ValueStateDescriptor<Roaring64NavigableMap> uvStateDescriptor = new ValueStateDescriptor<>("uv", Roaring64NavigableMap.class);
            StateTtlConfig stateTtlConfig = StateTtlConfig
                    .newBuilder(org.apache.flink.api.common.time.Time.seconds(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            pvStateDescriptor.enableTimeToLive(stateTtlConfig);
            uvStateDescriptor.enableTimeToLive(stateTtlConfig);

            pvState = this.getRuntimeContext().getState(pvStateDescriptor);
            uvState = this.getRuntimeContext().getState(uvStateDescriptor);
        }

        @Override
        public void process(Integer integer, Context context, Iterable<UserBehavior> elements, Collector<UserClickModel> out) throws Exception {
            Integer pv = pvState.value();
            Roaring64NavigableMap uv = uvState.value();
            if(uv == null){
                pv = 0;
                uv = new Roaring64NavigableMap();
            }
            for (UserBehavior u:elements) {
                pv = pv + 1;
                uv.add(u.getUserId());
            }
            pvState.update(pv);
            uvState.update(uv);
            UserClickModel userClickModel = new UserClickModel();
            userClickModel.setWindowEnd(context.currentWatermark());
            userClickModel.setCategoryId(integer);
            userClickModel.setPv(pv);
            userClickModel.setUv(uv.getIntCardinality());
            out.collect(userClickModel);
        }
    }

}
