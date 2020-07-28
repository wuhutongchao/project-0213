package com.atguigu.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Source03_Socket {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取端口数据
    val socketDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.打印
    socketDStream.print()

    //4.启动任务
    env.execute("Socket Source Test")


  }

}
