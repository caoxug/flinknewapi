//package cn.cxg
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.sources._
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.api._
//import org.apache.flink.types.Row
//import org.apache.flink.table.api.{TableEnvironment, TableSchema, Types, ValidationException}
//import java.sql.PreparedStatement
//import java.util.Properties
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//
////手动实现 interface 的方式来传入相关 JDBC Statement build 函数
//class CkSinkBuilder extends JdbcStatementBuilder[(String, Long, Float)] {
//  def accept(ps: PreparedStatement, v: (String, Long, Float)): Unit = {
//    ps.setString(1, v._1)
//    ps.setLong(2, v._2)
//    ps.setFloat(3, v._3)
//  }
//}
//
//object StreamingJob {
//  def main(args: Array[String]) {
//    val CkJdbcUrl = "jdbc:clickhouse://10.67.222.13:8123/default"
////    val CkUsername = "<your-username>"
////    val CkPassword = "<your-password>"
//    val BatchSize = 500 // 设置您的 batch size
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(3)
//    val prop = new Properties
//    prop.setProperty("bootstrap.servers", "10.67.222.10:9092,10.67.222.11:9092,10.67.222.12:9092,10.67.222.13:9092,10.67.222.14:9092")
//    prop.setProperty("group.id", "testcxg")
////    val str = env.addSource(new FlinkKafkaConsumer[String]("teacher",new SimpleStringSchema(),prop))
////
////    val insertIntoCkSql =
////      """
////        |  INSERT INTO sink_table (
////        |    name, grade, rate
////        |  ) VALUES (
////        |    ?, ?, ?
////        |  )
////      """.stripMargin
//
////将数据写入 ClickHouse  JDBC Sink
////    resultDataStream.addSink(
////      JdbcSink.sink[(String, Long, Float)](
////        insertIntoCkSql,
////        new CkSinkBuilder,
////        new JdbcExecutionOptions.Builder().withBatchSize(BatchSize).build(),
////        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
////          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
////          .withUrl(CkJdbcUrl)
//////          .withUsername(CkUsername)
//////          .withUsername(CkPassword)
////          .build()
////      )
////    )
//
//    env.execute("Flink DataStream to ClickHouse Example")
//  }
//}