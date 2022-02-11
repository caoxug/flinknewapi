//package com.bj58.che.sql;
//
//import com.bj58.che.entity.UserBehavior;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.table.api.*;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//import java.time.Duration;
//import java.util.Properties;
//
//import static org.apache.flink.table.api.Expressions.$;
//
///**
// * @author caoxuguang
// * @Description:
// * @date 2021/11/22 10:41 上午
// */
///*
//        -- 窗口1511650000
//        543461,1715,1464116,pv,1511658000
//        -- 窗口1511660000
//        543462,1715,1464116,pv,1511660001
//        543463,1715,1464116,pv,1511662000
//        543463,1715,1464116,pv,1511663000
//        --
//        543463,1715,1464116,pv,1511663001
//        543463,1715,1464116,pv,1511664003
//        543464,1715,1464116,pv,1511665000
//        -- 窗口1511670000
//        543465,1715,1464116,pv,1511675000
//        -- 2021年
//        543463,1715,1464116,pv,1637891398
// */
//public class ComputePvAndUvOnSql {
//    public static void main(String[] args) throws Exception{
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
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
//                        .withTimestampAssigner((event, timestamp) -> event.getDataTimestamp()*1000+8*60*60*1000));
//        SingleOutputStreamOperator<UserBehavior> pvData = dataStream.filter(data -> "pv".equals(data.getBehavior()));
//        tableEnv.createTemporaryView("pv_data",pvData,$("userId"),$("itemId"),$("categoryId"),$("behavior"),$("dataTimestamp").rowtime());
//
//        //        Table pv = tableEnv.sqlQuery("select count(*) from pv_data");
//        //        tableEnv.toRetractStream(pv,Types.ROW(Types.LONG())).print();
//        /*
//CREATE TABLE test.`uv` (
//  `day_str` varchar(100) NOT NULL,
//  `uv` bigint(10) DEFAULT NULL,
//  PRIMARY KEY (`day_str`)
//)
// */
//        String mysqlsql = "CREATE TABLE uv (\n" +
//                "  day_str STRING,\n" +
//                "  uv bigint,\n" +
//                "  PRIMARY KEY (day_str) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "   'connector' = 'jdbc',\n" +
//                "   'username' = 'root',\n" +
//                "   'password' = '123456',\n" +
//                "   'url' = 'jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&useSSL=false&autoReconnect=true',\n" +
//                "   'table-name' = 'uv',\n" +
//                "   'sink.buffer-flush.max-rows' = '1'\n" +
//                ")";
//        tableEnv.executeSql(mysqlsql);
//        tableEnv.executeSql("insert into uv select date_format(cast(dataTimestamp as string),'yyyyMMdd') as day_str,count(distinct userId) from pv_data group by date_format(cast(dataTimestamp as string),'yyyyMMdd')");
////        tableEnv.executeSql("insert into uv select cast(dataTimestamp as string) as day_str,1 from pv_data");
//
//        //        tableEnv.toRetractStream(uv,Types.ROW(Types.STRING(),Types.LONG())).print();
//
//        env.execute("ComputePvAndUvOnSql");
//    }
//}
