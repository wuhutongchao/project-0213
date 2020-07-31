package com.atguigu.window

import com.atguigu.bean.SensorReading
import com.atguigu.transform.MyReduce
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

object WindowType {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取数据源
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.KeyBy
    val sensorDStream: DataStream[SensorReading] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    val keyedDStream: KeyedStream[SensorReading, Tuple] = sensorDStream.keyBy(0)

    //4.开窗简写方式
    keyedDStream.timeWindow(Time.seconds(10))
    //    val windowDStream: WindowedStream[SensorReading, Tuple, TimeWindow] = keyedDStream.timeWindow(Time.seconds(15), Time.seconds(5))
    //    val windowDStream: WindowedStream[SensorReading, Tuple, GlobalWindow] = keyedDStream.countWindow(5L)
    //    keyedDStream.countWindow(10L, 2L)
    //
    //    //底层API
    val windowDStream: WindowedStream[SensorReading, Tuple, TimeWindow] = keyedDStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(6)))
    //    keyedDStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    //    keyedDStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))

    //增量处理窗口中的数据
    //    val result: DataStream[SensorReading] = windowDStream.reduce(new MyReduce)

    //全量处理窗口中的数据
    val result: DataStream[Int] = windowDStream.apply(new MyWindowFunction)

    //打印
    result.print()

    //启动任务
    env.execute("Window Test")

  }

}

class MyWindowFunction extends WindowFunction[SensorReading, Int, Tuple, TimeWindow] {

  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[Int]): Unit = {
    out.collect(input.size)
  }
}
