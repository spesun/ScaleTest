package com.test.scala

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sun on 2016/11/17.
  */
class ScalaTest {

}

object  ScalaTest {

  /**
    * 测试List。
    */
  def testList(): Unit = {
    var a = List(1, 2, 3, 4, 5, 6)
    println(1::a)
    //往后添加
    println(a :+ 100 )
    //往前添加
    println(100 +: a)
    //批量操作
    println(a++List(7))

    a.foreach(f => println(f + f))

    var numbers = ArrayBuffer(1,2,3)
    numbers += 5
    println(numbers)


    var array = ArrayBuffer[String]("0")
    var seq = Seq("1", "2")
    seq.foreach(t => {
      array += t
    })
    array += "3"
    println(array)

  }

  /**
    * 测试iterator
    */
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

  /**
    * 测试function
    *
    */
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

  /**
    * 测试 case class，类似于java bean
    */
  def testCaseClass(): Unit = {
    case class test (a:String);
    var d = test("a")
    println(d.a)
  }

  /**
    * 元组测试
    */
  def testTuple(): Unit = {
    var tuple = ("a" -> "b")
    println(tuple.toString())
    println(tuple._1)

  }

  /**
    * 测试map
   */
  def testMap(): Unit = {
    var map = Map(1 -> 2, 10 -> 20)
    //_代表map中的每一个元组.  _1为取元组中的第一个值
    println(map.filter(_._1.equals(1)).map(a => "aa").toList)
    println(map.filter(_._1.equals(1)).toList)

    //这里面不能使用_
    map.foreach(a => { println(a.toString()) })

//    println(map.get(1))
  }

  def main(args: Array[String]): Unit = {
    testList();
    //SparkIterator()
//    testFunction1()
//    testMap()

  }
}
