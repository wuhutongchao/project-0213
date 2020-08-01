package com.atguigu.state

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object State01_Test {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取数据源
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.转换为样例类
    val sensorDStream: DataStream[SensorReading] = lineDStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //4.分组
    val keyedDStream: KeyedStream[SensorReading, Tuple] = sensorDStream.keyBy("id")

    //5.实现如果连续2次采集的温度出现跳变,则报警
    //    val changerTempDStream: DataStream[(String, Double, Double)] = keyedDStream.map(new TempBigChangeMapFunction(10.0))
    val changerTempDStream: DataStream[(String, Double, Double)] = keyedDStream.flatMap(new TempBigChangeFlatMapFunction(10.0))
    //    val changerTempDStream: DataStream[(String, Double, Double)] = sensorDStream.map(new TempBigChangeMapFunction(10.0))

    //6.打印
    changerTempDStream.print()

    //7.启动
    env.execute("Map State Job")

  }

}

class TempBigChangeMapFunction(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {

  //定义状态用于存储上一次温度值
  var lastTemp: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def map(value: SensorReading): (String, Double, Double) = {

    //1.获取当前温度
    val curTemp: Double = value.temperature

    //2.获取上一次温度
    val lastT: Double = lastTemp.value()

    //3.更新状态
    lastTemp.update(curTemp)

    //4.判断温度是否出现跳变
    val diff: Double = (curTemp - lastT).abs
    if (diff > threshold) {
      (value.id, lastT, curTemp)
    } else {
      (value.id, 0.0, 0.0)
    }
  }
}

class TempBigChangeFlatMapFunction(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  //定义状态用于存储上一次温度值
  var lastTemp: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //1.获取当前温度
    val curTemp: Double = value.temperature

    //2.获取上一次温度
    val lastT: Double = lastTemp.value()

    //3.更新状态
    lastTemp.update(curTemp)

    //4.判断温度是否出现跳变
    val diff: Double = (curTemp - lastT).abs
    if (diff > threshold && lastT != 0.0) {
      out.collect(value.id, lastT, curTemp)
    }
  }
}

