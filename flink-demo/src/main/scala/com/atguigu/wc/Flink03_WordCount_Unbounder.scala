package com.atguigu.wc

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * 并行度设置,参数优先级
  * 1.在方法上调用的并行度(flatMap().setParallelism(2))
  * 2.在执行环境上设置的额并行度(env.setParallelism())
  * 3.提交任务的时候指定的并行度
  * 4.默认并行度(flink-conf.yaml)
  */
object Flink03_WordCount_Unbounder {

  def main(args: Array[String]): Unit = {

    //1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.disableOperatorChaining()

    //2.读取数据创建流
    val lineDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.扁平化
    val wordDS: DataStream[String] = lineDS.flatMap(_.split(" ")).startNewChain()

    //4.将每一个单词转换为元组
    val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_, 1)).disableChaining()

    //5.分组
    val keyedDS: KeyedStream[(String, Int), Tuple] = wordToOneDS.keyBy(0)

    //6.计算
    val result: DataStream[(String, Int)] = keyedDS.sum(1)

    //7.打印
    result.print()

    //8.启动任务
    env.execute()

  }

}
