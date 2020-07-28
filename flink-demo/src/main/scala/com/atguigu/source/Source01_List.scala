package com.atguigu.source

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._

object Source01_List {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.从集合加载数据
    val sensorDStream: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))

    //3.打印
    sensorDStream.print()

    //4.开启任务
    env.execute("List Source Test")


  }

}
