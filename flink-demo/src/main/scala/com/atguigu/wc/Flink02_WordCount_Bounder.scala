package com.atguigu.wc

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object Flink02_WordCount_Bounder {

  def main(args: Array[String]): Unit = {

    //1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //改变并行度
    env.setParallelism(2)

    //2.读取数据
    val lineDS: DataStream[String] = env.readTextFile("input")

    //3.扁平化
    val wordDS: DataStream[String] = lineDS.flatMap(_.split(" "))

    //4.将单词转换为元组
    val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_, 1))

    //5.分组
    val keyedDS: KeyedStream[(String, Int), Tuple] = wordToOneDS.keyBy(0)

    //6.统计单词出现的次数
    val result: DataStream[(String, Int)] = keyedDS.sum(1)

    //7.打印
    wordToOneDS.print("wordToOne")
    result.print("result")

    //8.启动任务
    env.execute("Stream Word Count")

  }


}
