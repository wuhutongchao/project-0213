package com.atguigu.sink

import java.util

import com.atguigu.bean.SensorReading
import com.atguigu.source.MySource
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object Sink03_ES {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.使用自定义数据源加载数据
    val sensorDStream: DataStream[SensorReading] = env.addSource(new MySource)

    //3.写入ES
    val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("hadoop102", 9200))

    val esSink: ElasticsearchSink[SensorReading] = new ElasticsearchSink.Builder[SensorReading](hosts, new MyEsSinkFunction).build()

    sensorDStream.addSink(esSink)

    //4.任务启动
    env.execute("ES Sink Test")

  }

}

class MyEsSinkFunction extends ElasticsearchSinkFunction[SensorReading] {
  override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

    val json = new java.util.HashMap[String, String]
    json.put("data", element.toString)

    val rqst: IndexRequest = Requests.indexRequest
      .index("panjinlian")
      .`type`("_doc")
      .source(json)

    indexer.add(rqst)

    println(element.toString)
    println("大郎，该吃药了！")
  }
}