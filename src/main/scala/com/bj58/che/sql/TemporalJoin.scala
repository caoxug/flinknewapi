//package com.bj58.che.sql
//import com.bj58.che.entity.UserClickModel
//import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
//import org.apache.flink.streaming.api.TimeCharacteristic
//
//import java.sql.Timestamp
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.api._
//
//import scala.collection.{JavaConversions, mutable}
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.types.Row
///**
// * @author CaoXuGuang
// * @Date: 2021/5/10 17:37
// * @Description: 维表join：Temporal table function join
// */
//object TemporalJoin {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tEnv = StreamTableEnvironment.create(env)
//    env.setParallelism(1)
//    // 设置时间类型是 event-time
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    // 构造订单数据
//    val ordersData = new mutable.MutableList[(Long, String, Timestamp)]
//    ordersData.+=((2L, "Euro", new Timestamp(2L)))
//    ordersData.+=((1L, "US Dollar", new Timestamp(3L)))
//    ordersData.+=((50L, "Yen", new Timestamp(4L)))
//    ordersData.+=((3L, "Euro", new Timestamp(5L)))
//    //构造汇率数据
//    val ratesHistoryData = new mutable.MutableList[(String, Long, Timestamp)]
//    ratesHistoryData.+=(("US Dollar", 102L, new Timestamp(1L)))
//    ratesHistoryData.+=(("Euro", 114L, new Timestamp(1L)))
//    ratesHistoryData.+=(("Yen", 1L, new Timestamp(1L)))
//    ratesHistoryData.+=(("Euro", 116L, new Timestamp(5L)))
//    ratesHistoryData.+=(("Euro", 119L, new Timestamp(7L)))
//
//    // 进行订单表 event-time 的提取
//    val orders = env
//      .fromCollection(ordersData)
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[((Long, String, Timestamp))]((Time.seconds(3))) {
//        override def extractTimestamp(element: (Long, String, Timestamp)): Long = {
//          element._3.getTime
//        }
//      })
//    //      .toTable(tEnv, 'amount, 'currency, 'rowtime.rowtime)
//    val ordersTable = tEnv.fromDataStream(orders,'amount, 'currency, 'rowtime.rowtime)
//
//    // 进行汇率表 event-time 的提取
//    val ratesHistory = env
//      .fromCollection(ratesHistoryData)
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[((String, Long, Timestamp))]((Time.seconds(3))) {
//        override def extractTimestamp(element: (String, Long, Timestamp)): Long = {
//          element._3.getTime
//        }
//      })
//    //      .toTable(tEnv, 'currency, 'rate, 'rowtime.rowtime)
//    val ratesHistoryTable = tEnv.fromDataStream(ratesHistory,'currency, 'rate, 'rowtime.rowtime)
//    // 注册订单表和汇率表
//    tEnv.registerTable("Orders", ordersTable)
//    tEnv.registerTable("RatesHistory", ratesHistoryTable)
//    val tab = tEnv.scan("RatesHistory");
//    // 创建TemporalTableFunction
//    val temporalTableFunction = tab.createTemporalTableFunction('rowtime, 'currency)
//    // 注册TemporalTableFunction
//    tEnv.registerFunction("Rates",temporalTableFunction)
//
//    val SQLQuery =
//      """
//        |SELECT o.currency, o.amount, r.rate,
//        |  o.amount * r.rate AS yen_amount
//        |FROM
//        |  Orders AS o,
//        |  LATERAL TABLE (Rates(o.rowtime)) AS r
//        |WHERE r.currency = o.currency
//        |""".stripMargin
//
//    tEnv.registerTable("TemporalJoinResult", tEnv.sqlQuery(SQLQuery))
//    val result: Table = tEnv.scan("TemporalJoinResult")
//    tEnv.toAppendStream[Row](result).print()
//    env.execute()
//  }
//}