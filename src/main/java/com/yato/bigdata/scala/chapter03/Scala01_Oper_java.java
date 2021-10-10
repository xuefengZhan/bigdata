package com.yato.bigdata.scala.chapter03;

public class Scala01_Oper_java {
    public static void main(String[] args) {
        String s1 = new String("abc");
        String s2 = "abc";
        String s3 = new String("abc");

        System.out.println(s1==s2);//false
        System.out.println(s1==s3);//false

    }
}
