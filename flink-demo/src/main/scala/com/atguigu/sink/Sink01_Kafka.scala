package com.atguigu.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Sink01_Kafka {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取端口数据
    val socketDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.写入kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    socketDStream.addSink(new FlinkKafkaProducer011[String]("first", new SimpleStringSchema(), properties))

    //4.打印
    socketDStream.print()

    //5.启动
    env.execute("Kafka Sink Test")

  }

}
