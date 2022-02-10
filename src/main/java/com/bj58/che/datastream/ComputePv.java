//package com.bj58.che.datastream;
//
//import com.bj58.che.entity.PageViewCount;
//import com.bj58.che.entity.UserBehavior;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.util.Collector;
//
//import java.time.Duration;
//import java.util.Properties;
//
///**
// * @author caoxuguang
// * @Description:
// * @date 2021/11/17 4:06 下午
// */
//public class ComputePv {
//    public static void main(String[] args) throws Exception{
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        Properties prop = new Properties();
//        prop.setProperty("bootstrap.servers","localhost:9092");
//        prop.setProperty("group.id","cxg");
//        FlinkKafkaConsumer<String> fkc = new FlinkKafkaConsumer<String>("test01",new SimpleStringSchema(),prop);
//        DataStreamSource<String> inputStream = env.addSource(fkc);
//        DataStream<UserBehavior> dataStream = inputStream
//                .map(line -> {
//                    String[] fields = line.split(",");
//                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
//                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                        .withTimestampAssigner((event, timestamp) -> event.getDataTimestamp()));
//        SingleOutputStreamOperator<UserBehavior> pvData = dataStream.filter(data -> "pv".equals(data.getBehavior()));
//        //方式一：单并行度，会数据倾斜
//        SingleOutputStreamOperator<Tuple2<String, Long>> pv1 = pvData
//                .map(data -> new Tuple2<String,Long>("pv", 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .keyBy(data -> data.f0)
//                .timeWindow(Time.seconds(10))
//                .sum(1);
//        pv1.print();
//        //方式二：多并行度，解决数据倾斜
//        SingleOutputStreamOperator<PageViewCount> pvStream = pvData
//                .map(data -> new Tuple2<Integer, Long>((int) (Math.random() * 10), 1L)).returns(Types.TUPLE(Types.INT, Types.LONG))
//                .keyBy(data -> data.f0)
//                .timeWindow(Time.seconds(10))
//                .aggregate(new PvCountAgg(), new PvCountResult());
//        DataStream<PageViewCount> pv2 = pvStream
//                .keyBy(PageViewCount::getWindowEnd)
//                .process(new TotalPvCount());
//        pv2.print();
//        env.execute("ComputePv");
//    }
//
//    // 实现自定义预聚合函数
//    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
//        @Override
//        public Long createAccumulator() {
//            return 0L;
//        }
//
//        @Override
//        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
//            return accumulator + 1;
//        }
//
//        @Override
//        public Long getResult(Long accumulator) {
//            return accumulator;
//        }
//
//        @Override
//        public Long merge(Long a, Long b) {
//            return a + b;
//        }
//    }
//
//    // 实现自定义窗口,泛型为输入类型、输出类型、key类型、窗口类型
//    public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
//        @Override
//        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
//            out.collect( new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()) );
//        }
//    }
//
//    // 实现自定义处理函数，把相同窗口分组统计的count值叠加
//    public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
//        // 定义状态，保存当前的总count值
//        ValueState<Long> totalCountState;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class,0L));
//        }
//
//        @Override
//        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
//            totalCountState.update( totalCountState.value() + value.getCount() );
//            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
//        }
//
//        @Override
//        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
//            // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
//            Long totalCount = totalCountState.value();
//            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
//            // 清空状态
//            totalCountState.clear();
//        }
//    }
//}
