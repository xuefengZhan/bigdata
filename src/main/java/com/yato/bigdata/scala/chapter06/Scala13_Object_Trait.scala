package com.yato.bigdata.scala.chapter06

object Scala13_Object_Trait {



  trait Runnable {
    def run(): Unit

    def test() : Unit = {}
  }


  class Person extends Runnable {
    def run(): Unit = {
    }

    override def test() : Unit = {}

  }


}
