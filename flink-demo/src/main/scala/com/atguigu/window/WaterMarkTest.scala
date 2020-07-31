package com.atguigu.window

import com.atguigu.bean.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WaterMarkTest {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500)

    //设置系统使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据源
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.KeyBy
    val sensorDStream: DataStream[SensorReading] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //4.获取数据时间并指定WaterMark的偏移
    val waterMarkSensorDStream: DataStream[SensorReading] = sensorDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timestamp * 1000L
      }
    })

    //5.keyBy
    val keyedDStream: KeyedStream[SensorReading, Tuple] = waterMarkSensorDStream.keyBy("id")

    //6.开窗
    val windowDStream: WindowedStream[SensorReading, Tuple, TimeWindow] = keyedDStream.timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(new OutputTag[SensorReading]("late"))

    //7.计算并打印
    val result = windowDStream.apply(new MyWindowFunction2)
    result.print("Window")
    result.getSideOutput(new OutputTag[SensorReading]("late")).print("side")

    //8.执行任务
    env.execute("WaterMark Job")

  }

}

class MyWindowFunction2 extends WindowFunction[SensorReading, (Long, Long, Int), Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Long, Int)]): Unit = {
    out.collect((window.getStart, window.getEnd, input.size))
  }
}