package cn.cxg.sql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author caoxuguang
 * @Description:
 * @date 2022/2/11 4:02 下午
 */
public class KafkaConnectOnSql {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String ddlSource = "CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 'test01',\n" +
                "    'connector.startup-mode' = 'latest-offset',\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format.type' = 'csv'\n" +
                ")";

        tableEnv.executeSql(ddlSource);
//        String selectSql = "select user_id,item_id,category_id,behavior from user_behavior";
//        Table seTable = tableEnv.sqlQuery(selectSql);
//        tableEnv.toRetractStream(seTable,Types.TUPLE(Types.LONG, Types.LONG, Types.LONG,Types.STRING)).print();

        String countSql = "select user_id, count(user_id) from user_behavior group by user_id";
        Table resultTable = tableEnv.sqlQuery(countSql);
        tableEnv.toRetractStream(resultTable, Types.TUPLE(Types.LONG,Types.LONG)).print();

        env.execute("KafkaConnectOnSql");
    }
}
