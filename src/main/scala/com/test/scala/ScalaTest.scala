package com.test.scala

/**
  * Created by sun on 2016/11/17.
  */
class ScalaTest {

}

object  ScalaTest {

  def testList(): Unit = {
    var a = List(1, 2, 3, 4, 5, 6)
    a.foreach(f => println(f + f))
  }

  def SparkIterator(): Unit = {
    var a = Iterator(1, 2)

    val b = 1.1

    // var b = 1.1
    // Error:scalac: Error: object VolatileBooleanRef does not have a member create

    a.foreach(e =>  println(b + e))
    a.foreach(_*2)

   //TODO 如何让a重新循环
    var c = a.map(_*2)
    c.foreach(e => println(e))
  }

  def testFunction1(): Unit = {
    var fun = new Function1[Int, Int] {
      //override def apply(v1: Int): Int = ???
      override def apply(v1: Int): Int = v1 * 3
    }

    println(fun.apply(5))


    var funnew = (x: Int) =>  x + 2

    println(fun.andThen(funnew).apply(5))
    println(fun.compose(funnew).apply(5))
  }

  def main(args: Array[String]): Unit = {
    //testList();
    //SparkIterator()
    testFunction1()

  }
}
