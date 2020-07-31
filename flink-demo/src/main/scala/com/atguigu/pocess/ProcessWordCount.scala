package com.atguigu.pocess

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.读取数据源
    val lineDStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    //3.flatMap  line==>[word,word...]
    val wordDStream: DataStream[String] = lineDStream.process(new MyFlatProcessFunction)

    //4.map   word===>(word,1)
    val wordToOneDStream: DataStream[(String, Int)] = wordDStream.process(new MyMapProcessFunction)

    //5.分组
    val keyedDStream: KeyedStream[(String, Int), Tuple] = wordToOneDStream.keyBy(0)

    //6.统计单词出现的次数
    val wordToSumDStream: DataStream[(String, Int)] = keyedDStream.process(new MyReduceProcessFunction)

    //7.打印
    wordToSumDStream.print()

    //8.启动任务
    env.execute("Process WordCount Job")

  }

}

class MyFlatProcessFunction extends ProcessFunction[String, String] {
  override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
    val words: Array[String] = value.split(" ")
    words.foreach(word => {
      out.collect(word)
    })
  }
}

class MyMapProcessFunction extends ProcessFunction[String, (String, Int)] {

  override def processElement(value: String, ctx: ProcessFunction[String, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
    out.collect((value, 1))
  }
}

class MyReduceProcessFunction extends KeyedProcessFunction[Tuple, (String, Int), (String, Int)] {

  lazy val sum: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("keyed-sum", classOf[Int]))

  //按照key指定状态
  //  private var sum: ValueState[Int] = _
  //
  //  override def open(parameters: Configuration): Unit = {
  //    sum = getRuntimeContext.getState(new ValueStateDescriptor[Int]("keyed-sum", classOf[Int]))
  //  }

  override def processElement(value: (String, Int), ctx: KeyedProcessFunction[Tuple, (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {

    //1.取出状态中的数据
    val lastSum: Int = sum.value()
    val curSum: Int = lastSum + value._2

    //2.更新状态
    sum.update(curSum)

    //3.将数据写出
    out.collect((value._1, curSum))

  }
}