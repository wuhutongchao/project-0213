package com.atguigu.test

import scala.util.Random

object Test {

  def main(args: Array[String]): Unit = {

    val random = new Random()

    println(random.nextGaussian())
    println(random.nextGaussian())
    println(random.nextGaussian())
    println(random.nextGaussian())
    println(random.nextGaussian())
    println(random.nextGaussian())
    println(random.nextGaussian())

  }

}
