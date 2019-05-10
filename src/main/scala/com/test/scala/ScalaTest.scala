package com.test.scala

import scala.collection.mutable.ArrayBuffer
import org.junit.Test

/**
  * Created by sun on 2016/11/17.
  */
@Test
class ScalaTest {

  @Test
  //TODO 不能使用单元测试
  def test(): Unit = {

  }
}

@Test
object  ScalaTest {

  /**
    * 测试List。
    */
  @Test
  def testList(): Unit = {
    var a = List(1, 2, 3, 4, 5, 6)
    println(1::a)
    //往后添加
    println(a :+ 100 )
    //往前添加
    println(100 +: a)
    //批量操作
    println(a++List(7))

//    a.foreach(f => println(f + f))

    //ArrayBuffer测试
    var numbers = ArrayBuffer(1,2,3)
    numbers += 5
    println(numbers)

    var array = ArrayBuffer[String]("0")
    var seq = 1 to 3
    seq.foreach(t => {
      array += t.toString()
    })
    array += "4"
    println(array)

    //map方法测试
    var a2 = array.map(line => {
      //不能使用return, 否则会直接退出
//      return line + "new"
      line + "new"
    })
    println(a2)

  }

  /**
    * 测试iterator
    */
  @Test
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

    //先执行哪个函数的关系
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
  @Test
  def testMap(): Unit = {
    var map = Map(1 -> 2, 10 -> 20)
    //_代表map中的每一个元组.  _1为取元组中的第一个值
    println(map.filter(_._1.equals(1)).map(a => "aa").toList)
    println(map.filter(_._1.equals(1)).toList)

    //这里面不能使用_
    map.foreach(a => { println(a.toString()) })

//    println(map.get(1))
  }


  //反射测试
  //https://stackoverflow.com/questions/3213510/what-is-a-manifest-in-scala-and-when-do-you-need-it
  //https://blog.csdn.net/hellojoy/article/details/81064002
  //https://www.cnblogs.com/tiger-xc/p/6006512.html
  def testReflection(): Unit = {

    //1、原始的写法
    def foo[T](x: List[T], m: Manifest[T]) = {
      if (m <:< manifest[String])
        println("Hey, this list is full of strings")
      else
        println("Non-stringy list")
    }

    foo(List("one", "two"), manifest[String]) // Hey, this list is full of strings
    foo(List(1, 2), manifest[Int]) // Non-stringy list
    foo(List("one", 2), manifest[Any]) // Non-stringy list
    println("=======1")

    //2、隐式转换
    def foo2[T](x: List[T])(implicit m: Manifest[T]) = {
      if (m <:< manifest[String])
        println("Hey, this list is full of strings")
      else
        println("Non-stringy list")
    }

    foo2(List("one", "two")) // Hey, this list is full of strings
    foo2(List(1, 2)) // Non-stringy list
    foo2(List("one", 2)) // Non-stringy list
    println("=======2")

    //3、使用Context bounds, 可以简化为
    def foo3[T:Manifest](x: List[T])= {
      if (manifest[T]  <:< manifest[String])
        println("Hey, this list is full of strings")
      else
        println("Non-stringy list")
    }

    foo3(List("one", "two")) // Hey, this list is full of strings
    foo3(List(1, 2)) // Non-stringy list
    foo3(List("one", 2)) // Non-stringy list
    println("=======3")

    //4、TypeTag测试
    import scala.reflect.ClassTag
    val ru = scala.reflect.runtime.universe
    def foo4[T:ru.TypeTag](x: List[T])= {
//    def foo4[T](x: List[T], t:ru.TypeTag[T])= {
      if(ru.typeOf[T] <:< ru.typeOf[String])
        println("Hey, this list is full of strings")
      else
        println("Non-stringy list")
    }
    foo4(List("one", "two")) // Hey, this list is full of strings
    foo4(List(1, 2)) // Non-stringy list
    foo4(List("one", 2)) // Non-stringy list
    println("=======4")
  }

  def main(args: Array[String]): Unit = {
//    testList();
    //SparkIterator()
//    testFunction1()
//    testMap()
    testReflection()
  }
}
