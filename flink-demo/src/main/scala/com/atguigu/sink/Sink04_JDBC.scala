package com.atguigu.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.bean.SensorReading
import com.atguigu.source.MySource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object Sink04_JDBC {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取数据
    val sensorDStream: DataStream[SensorReading] = env.addSource(new MySource)

    //3.写入MySQL
    sensorDStream.addSink(new MyJDBCSinkFunction)

    //4.启动任务
    env.execute("JDBC Sink Test")

  }

}

class MyJDBCSinkFunction extends RichSinkFunction[SensorReading] {

  //声明MySQL连接&预编译SQL语句
  var connection: Connection = _
  var insertPstm: PreparedStatement = _
  var updatePstm: PreparedStatement = _

  //创建JDBC连接
  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "000000")
    insertPstm = connection.prepareStatement("INSERT INTO sensor(id,temperature) VALUES(?,?)")
    updatePstm = connection.prepareStatement("UPDATE sensor SET temperature=? WHERE id=?")
  }

  //使用JDBC连接,将每一行写入Mysql
  override def invoke(value: SensorReading): Unit = {

    updatePstm.setDouble(1, value.temperature)
    updatePstm.setString(2, value.id)
    updatePstm.execute()

    //当前没有对应sensorID的数据
    if (updatePstm.getUpdateCount == 0) {
      insertPstm.setString(1, value.id)
      insertPstm.setDouble(2, value.temperature)
      insertPstm.execute()
    }

  }

  //结尾,释放连接
  override def close(): Unit = {
    insertPstm.close()
    updatePstm.close()
    connection.close()
  }

}
