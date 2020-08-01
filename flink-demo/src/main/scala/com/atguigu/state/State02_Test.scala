package com.atguigu.state

import java.util

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed

object State02_Test {

  def main(args: Array[String]): Unit = {

  }

}

class MyStateFunction extends RichMapFunction[String, String] with ListCheckpointed[Long] {

  //1.值状态
  lazy val valueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("value-state", classOf[Long]))

  //2.集合状态
  lazy val listState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("list-state", classOf[String]))

  //3.Map状态
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("map-state", classOf[String], classOf[Double]))

  //4.Reduce状态
  lazy val reduceState: ReducingState[String] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[String]("reduce-state", new MyStateReduce, classOf[String]))

  var count: Long = 0L

  override def map(value: String): String = {

    //1.值状态的使用
    valueState.value()
    valueState.update(0L)
    valueState.clear()

    //2.集合状态的使用
    listState.add("")
    listState.update(new util.ArrayList[String]())
    listState.addAll(new util.ArrayList[String]())
    listState.clear()

    //3.Map状态
    mapState.put("", 0.0)
    mapState.values()
    mapState.isEmpty
    mapState.putAll(new util.HashMap[String, Double]())
    mapState.remove("")
    mapState.clear()

    //4.Reduce状态
    reduceState.add("")
    reduceState.get()
    reduceState.clear()

    ""
  }

  override def restoreState(state: util.List[Long]): Unit = {
    val iter: util.Iterator[Long] = state.iterator()
    while (iter.hasNext) {
      count += iter.next()
    }
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] = {
    val list = new util.ArrayList[Long]()
    list.add(count)
    list
  }
}

class MyStateReduce extends ReduceFunction[String] {
  override def reduce(value1: String, value2: String): String = {
    value1 + value2
    Math.max(value1.length, value2.length).toString
  }
}