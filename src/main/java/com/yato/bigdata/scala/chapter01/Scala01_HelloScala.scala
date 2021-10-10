package com.yato.bigdata.scala.chapter01

/**
 *  Object : 声明类的关键字；  在声明类的同时可以声明对象   类名也是对象名
 *           class 也是声明类的关键字，但是不会同时声明对象
 *  def:    用于声明函数(方法)的关键字；
 *  main:  方法名称
 *
 *
 *  (args: Array[String]) ：
 *  方法名后面的小括号里是参数声明，多个参数之间用逗号隔开；
 *
 *  args: Array[String] ：
 *  参数后面紧跟着的是参数类型，参数和参数类型用冒号隔开，参数名称在前
 *  体现了和java不同的风格：
 *      java是强类型语言，在运行之前，所有数据都要明确类型
 *      scala也是 强类型语言，scala更关心如何使用参数，java更关心类型
 *
 *  Array[String]：
 *  Scala是完全面向对象的语言，数组在scala中是Array类
 *  Java中的数组并不是一个类，String[]  s  只是表示a变量存储String类型数组地址
 *
 *  和 参数：参数类型  这种风格一致，Scala中定义方法的时候
 *  方法返回值类型在方法名后面 =>   test() : void
 *
 *  Unit: Scala是完全面向对象的语言，java中用关键字void表示没有返回值    Scala用Unit类型表示没有返回值
 *
 *  \=： 表示赋值,体现了Scala万物皆对象  将方法体对象 赋值给 函数
 *
 *  {} ： 方法体
 *
 *  Scala是基于java开发的，java的代码可以直接在scala中使用
 *
 *  scala会省略分号
 *   一般你每一行完成一段逻辑的代码编写
 *   如果一行写多个逻辑，那么需要用分号隔开
 *
 *
 */
object Scala01_HelloScala {
  def main(args: Array[String]): Unit = {
    System.out.println("Hello Scala")
  }
}
