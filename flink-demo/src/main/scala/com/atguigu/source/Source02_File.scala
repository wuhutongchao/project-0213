package com.atguigu.source

import org.apache.flink.streaming.api.scala._

object Source02_File {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取文件
    val sensorDStream: DataStream[String] = env.readTextFile("input/sensor.txt")

    //3.打印
    sensorDStream.print()

    //4.启动任务
    env.execute("File Source Test")

  }

}
