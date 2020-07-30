//package com.atguigu.sink
//
//import com.atguigu.bean.SensorReading
//import com.atguigu.source.MySource
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.redis.RedisSink
//import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
//import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
//
//object Sink02_Redis {
//
//  def main(args: Array[String]): Unit = {
//
//    //1.创建执行环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    //2.使用自定义数据源加载数据
//    val sensorDStream: DataStream[SensorReading] = env.addSource(new MySource)
//
//    //3.写入Redis
//    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig
//    .Builder()
//      .setHost("hadoop102")
//      .setPort(6379)
//      .build()
//
//    sensorDStream.addSink(new RedisSink[SensorReading](config, new MyRedisMapper))
//
//    //4.启动
//    env.execute("Redis Sink Test")
//
//  }
//
//}
//
//class MyRedisMapper extends RedisMapper[SensorReading] {
//
//  override def getCommandDescription: RedisCommandDescription = {
//    new RedisCommandDescription(RedisCommand.HSET, "sensor")
//  }
//
//  override def getKeyFromData(data: SensorReading): String = {
//    data.id
//  }
//
//  override def getValueFromData(data: SensorReading): String = {
//    data.temperature.toString
//  }
//
//
//}
