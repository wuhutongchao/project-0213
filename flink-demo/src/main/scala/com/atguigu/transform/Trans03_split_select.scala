package com.atguigu.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._

object Trans03_split_select {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取端口数据
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.使用Map将一行数据转换为样例类对象
    val sensorDStream: DataStream[SensorReading] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //4.根据温度分流
    val splitDStream: SplitStream[SensorReading] = sensorDStream.split(sensor => {
      if (sensor.temperature > 30) {
        Seq("High")
      } else {
        Seq("Low")
      }
    })

    //5.选择分支
    val highDStream: DataStream[SensorReading] = splitDStream.select("High")
    val lowDStream: DataStream[SensorReading] = splitDStream.select("Low")
    val allDStream: DataStream[SensorReading] = splitDStream.select("High", "Low")

    //6.打印
    highDStream.print("High")
    lowDStream.print("Low")
    allDStream.print("All")

    //7.启动任务
    env.execute("Split Select Test")

  }

}
