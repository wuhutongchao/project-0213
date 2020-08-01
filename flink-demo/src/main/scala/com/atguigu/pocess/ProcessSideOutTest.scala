package com.atguigu.pocess

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessSideOutTest {

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

    //4.使用ProcessFunction分流
    val highTempStream: DataStream[(String, Double, String)] = sensorDStream.process(new SplitProcessFunction(30.0))

    //5.获取侧输出流
    val lowTempStream: DataStream[(String, String)] = highTempStream.getSideOutput(new OutputTag[(String, String)]("lowTemp"))

    //6.打印
    highTempStream.print("high")
    lowTempStream.print("low")

    //7.启动任务
    env.execute("Process Side Out Job")

  }

}

class SplitProcessFunction(threshold: Double) extends ProcessFunction[SensorReading, (String, Double, String)] {

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, (String, Double, String)]#Context, out: Collector[(String, Double, String)]): Unit = {

    //取出数据中的温度
    val temperature: Double = value.temperature

    //根据温度高低将数据输出到不同的流中
    if (temperature > threshold) {
      //写入主流
      out.collect((value.id, temperature, "warning"))
    } else {
      //写入侧输出流
      ctx.output(new OutputTag[(String, String)]("lowTemp"), (value.id, "healthy"))
    }

  }
}