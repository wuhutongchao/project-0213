package com.atguigu.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object Trans02_KeyBy {

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

    //4.按照传感器id分组
    //    val keyedDStream: KeyedStream[SensorReading, Tuple] = sensorDStream.keyBy(0)
    //    val keyedDStream: KeyedStream[SensorReading, Tuple] = sensorDStream.keyBy("id")
    //    val keyedDStream: KeyedStream[SensorReading, String] = sensorDStream.keyBy(x => x.id)
    val keyedDStream: KeyedStream[SensorReading, String] = sensorDStream.keyBy(new MyKeySelector)

    //5.计算每一个传感器的最高温度
    //    val result: DataStream[SensorReading] = keyedDStream.maxBy(2)

    //计算每一个传感器的最大时间和最低温度
    //    val result: DataStream[SensorReading] = keyedDStream.reduce((x, y) =>
    //      SensorReading(x.id, x.timestamp.max(y.timestamp), x.temperature.min(y.temperature))
    //    )
    val result: DataStream[SensorReading] = keyedDStream.reduce(new MyReduce)

    //6.打印
    result.print()

    //7.启动任务
    env.execute("KeyBy Test")

  }

}

class MyKeySelector extends KeySelector[SensorReading, String] {
  override def getKey(value: SensorReading): String = value.id
}

class MyReduce extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id,
      value1.timestamp.max(value2.timestamp),
      value1.temperature.min(value2.temperature))
  }
}