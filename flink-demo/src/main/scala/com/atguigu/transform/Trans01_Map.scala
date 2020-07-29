package com.atguigu.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object Trans01_Map {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取端口数据
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.使用Map将一行数据转换为样例类对象
    //    val sensorDStream: DataStream[SensorReading] = lineDStream.map(line => {
    //      val arr: Array[String] = line.split(",")
    //      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    //    })

    //    val sensorDStream: DataStream[SensorReading] = lineDStream.map(new MyMapFunc)
    val sensorDStream: DataStream[SensorReading] = lineDStream.map(new MyRichMapFunc)

    //4.打印
    sensorDStream.print()


    //5.开启任务
    env.execute("Tans Map Test")

  }
}


class MyMapFunc extends MapFunction[String, SensorReading] {
  override def map(value: String): SensorReading = {
    val arr: Array[String] = value.split(",")
    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
  }
}

class MyRichMapFunc extends RichMapFunction[String, SensorReading] {

  override def map(value: String): SensorReading = {
    val arr: Array[String] = value.split(",")
    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
  }

}