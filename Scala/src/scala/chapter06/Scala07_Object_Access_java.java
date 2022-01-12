package scala.chapter06;

//public class Scala07_Object_Access_java {
//    public static void main(String[] args) {
//        A a = new A();
//        a.clone();
//
//        //方法的提供者和调用者的关系
//        //方法的提供者：java.lang.Object
//        //方法的调用者：com.yato.bigdata.scala.chapter06.Scala07_Object_Access_java
//
//        // . 点 不是调用的意思  而是表示从属关系
//        //所以这里调用者不是对象a
//        //而是在com.yato.bigdata.scala.chapter06.Scala07_Object_Access_java类中的main方法中调用了对象a的clone方法
//
//
//        //clone()是protected方法,  调用者和提供者不是同包、同类，那么就要判断是不是父子类
//        //不是父子类，所以这里不能调用a的clone()
//        //张三的父亲 是不是  李四的父亲
//        // 因为A类的父类 Object 和 Scala07_Object_Access_java类的父类Object 不是同一个
//
//    }
//}
//class A{
//
//}
