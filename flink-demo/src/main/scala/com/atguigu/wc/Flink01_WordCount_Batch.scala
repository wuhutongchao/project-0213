package com.atguigu.wc

import org.apache.flink.api.scala._

object Flink01_WordCount_Batch {

  def main(args: Array[String]): Unit = {

    /**
      * SparkCore实现WordCount
      * 1.sparkConf-->SparkContext (sc)
      * 2.sc.textFile().flatMap().map().reduceByKey.collect.print
      */

    //1.获取Flink执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.读取文件中的数据
    val lineDS: DataSet[String] = env.readTextFile("input")

    //3.扁平化
    val wordDS: DataSet[String] = lineDS.flatMap(_.split(" "))

    //4.将单词转换为元组
    val wordToOne: DataSet[(String, Int)] = wordDS.map((_, 1))

    //5.分组
    val groupDS: GroupedDataSet[(String, Int)] = wordToOne.groupBy(0)

    //6.计算单词出现的次数
    val result: AggregateDataSet[(String, Int)] = groupDS.sum(1)

    //7.打印
    result.print()

  }

}
