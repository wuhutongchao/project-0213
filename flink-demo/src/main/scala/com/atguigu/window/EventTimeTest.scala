package com.atguigu.window

import com.atguigu.bean.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/*
使用事件时间语义的流程
1.设置系统使用事件时间
2.告诉系统数据中哪个字段是时间戳字段
 */
object EventTimeTest {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置系统使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据源
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.KeyBy
    val sensorDStream: DataStream[SensorReading] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //告诉系统数据中哪个字段是时间戳字段
    val eventTimeSensorDStream: DataStream[SensorReading] = sensorDStream.assignAscendingTimestamps(sensor => {
      println(sensor.timestamp)
      sensor.timestamp * 1000L
    })

    //keyby
    val keyedStream: KeyedStream[SensorReading, Tuple] = eventTimeSensorDStream.keyBy("id")

    //设置滚动窗口
    val windowDStream: WindowedStream[SensorReading, Tuple, TimeWindow] = keyedStream
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(new OutputTag[SensorReading]("lateData"))

    val result: DataStream[Int] = windowDStream.apply(new MyWindowFunction)

    //获取侧输出流
    val sideDStream: DataStream[SensorReading] = result.getSideOutput(new OutputTag[SensorReading]("lateData"))

    //打印
    sensorDStream.print("Data")
    result.print("Window")
    sideDStream.print("side")

    //启动任务
    env.execute("Event Time Job")

  }

}

