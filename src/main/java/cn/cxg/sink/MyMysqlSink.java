//package com.bj58.che.sink;
//
//import com.bj58.che.entity.UserClickModel;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//
//import java.sql.*;
//
///**
// * @author caoxuguang
// * @Description:
// * @date 2021/11/19 6:53 下午
// */
//public class MyMysqlSink extends RichSinkFunction<UserClickModel> {
//    Connection connection;
//    PreparedStatement ps;
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        String driver = "com.mysql.jdbc.Driver";
//        String url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&useSSL=false&autoReconnect=true";
//        String username = "root";
//        String password = "123456";
//        Class.forName(driver);
//        connection = DriverManager.getConnection(url, username, password);
//        String sql = "insert into pv_and_uv values(?,?,?,?);";
//        ps = connection.prepareStatement(sql);
//    }
//
//    @Override
//    public void invoke(UserClickModel value, Context context) throws Exception {
//        try{
//            ps.setLong(1,value.getWindowEnd());
//            ps.setInt(2, value.getCategoryId());
//            ps.setInt(3, value.getPv());
//            ps.setInt(4, value.getUv());
//            ps.executeUpdate();
//        }catch (Exception e){
//            System.out.println(e);
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        if (connection != null) {
//            connection.close();
//        }
//        if (ps != null) {
//            ps.close();
//        }
//    }
//}
