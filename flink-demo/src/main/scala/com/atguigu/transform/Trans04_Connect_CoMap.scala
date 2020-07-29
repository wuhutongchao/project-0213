package com.atguigu.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._

object Trans04_Connect_CoMap {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
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
    val lowDStream: DataStream[(String, String)] = splitDStream.select("Low").map(sensor => (sensor.id, "healthy"))

    //6.连接2个流
    val connectDStream: ConnectedStreams[SensorReading, (String, String)] = highDStream.connect(lowDStream)

    //7.将2个流真正连接到一起
    val result: DataStream[Object] = connectDStream.map(
      highData => (highData.id, highData.temperature, "warninng"),
      lowData => lowData
    )

    //8.打印
    result.print()

    //9.启动任务
    env.execute("Connect Job")

  }

}
