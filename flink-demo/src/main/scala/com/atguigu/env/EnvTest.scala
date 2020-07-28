package com.atguigu.env

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object EnvTest {

  def main(args: Array[String]): Unit = {

    //1.批处理
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchLocalEnv: ExecutionEnvironment = ExecutionEnvironment.createLocalEnvironment(2)
    val batchRemoteEnv: ExecutionEnvironment = ExecutionEnvironment.createRemoteEnvironment("", 6123, "")

    //2.流处理
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    StreamExecutionEnvironment.createLocalEnvironment(2)
    StreamExecutionEnvironment.createRemoteEnvironment("", 6123, "")

  }


}
