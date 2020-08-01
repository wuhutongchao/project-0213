package com.atguigu.pocess

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessSensorTest {

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

    //5.触发计算
    keyedDStream.process(new MyKeyedProcessFunction).print()

    //6.启动
    env.execute()
  }

}


class MyKeyedProcessFunction extends KeyedProcessFunction[Tuple, SensorReading, String] {

  //定义上一条数据的温度状态
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))

  //定义闹钟触发时间的状态
  lazy val ts: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {

    //获取当前数据的温度
    val curTemp: Double = value.temperature
    //获取上一次的温度
    val lastT: Double = lastTemp.value()
    //状态更新
    lastTemp.update(curTemp)

    //比较当前温度及上次温度
    if (curTemp >= lastT && ts.value() == 0) {
      //获取当前时间
      val curTs: Long = ctx.timerService().currentProcessingTime()

      //保存闹钟状态
      ts.update(curTs + 10 * 1000L)

      //定义10秒后的闹钟
      ctx.timerService().registerProcessingTimeTimer(curTs + 10 * 1000L)

    } else if (curTemp < lastT) {

      //删除闹钟
      ctx.timerService().deleteProcessingTimeTimer(ts.value())

      //清空状态
      ts.clear()
    }
  }

  //闹钟响了,触发的操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

    out.collect("连续10秒温度没有下降 ! ")

    //清空状态
    ts.clear()
  }
}