package com.atguigu.source

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

object Source05_MySource {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.使用自定义的Source接收数据
    val sensorDStream: DataStream[SensorReading] = env.addSource(new MySource)

    //3.打印
    sensorDStream.print()

    //4.启动任务
    env.execute("MySource Test")

  }

}

class MySource extends SourceFunction[SensorReading] {

  //定义一个标志位用于控制是否接收数据
  var running = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    //定义随机数,用于随机生成温度
    val random = new Random()

    //定义第一次温度
    val firstTemp: immutable.IndexedSeq[(String, Double)] = (1 to 5).map(i => {
      (s"sensor_$i", 30 + random.nextGaussian() * 10)
    })

    while (running) {

      //每一次温度的变化
      val sensorReadings: immutable.IndexedSeq[SensorReading] = firstTemp.map(t => {
        SensorReading(t._1, System.currentTimeMillis(), t._2 + random.nextGaussian() * 10)
      })

      //将数据写出
      sensorReadings.foreach(sensorReading => {
        ctx.collect(sensorReading)
      })

      Thread.sleep(2000)

    }

  }

}
