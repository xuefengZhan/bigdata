package com.yato.bigdata.scala.chapter06;

public class Scala08_Object_Method_Java {

    public static void main(String[] args) {
        AAA08 a = new AAA08();
        System.out.println(a.sum());//20

        BBB08 b = new BBB08();
        System.out.println(b.sum());//40

        AAA08 c = new BBB08();
        System.out.println(c.sum());//40

        //注释掉BBB08中的sum()方法，执行下面的代码
        AAA08 d = new BBB08();
        System.out.println(d.sum());//20

        //注释掉BBB08中的sum()方法，执行下面的代码
        AAA08 e = new BBB08();
        System.out.println(e.sum1());//30

    }
}

class AAA08 {
    public int i = 10;

    public int sum() {
        return i + 10;
    }

    public int sum1() {
        return getI() + 10;
    }

    public int getI() {
        return i;
    }
}

class BBB08 extends AAA08 {
    public int i = 20;

    //    public int sum(){
//        return i + 20;
//    }
    public int getI() {
        return i;
    }
}