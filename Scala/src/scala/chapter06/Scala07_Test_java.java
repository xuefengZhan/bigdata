package scala.chapter06;

public class Scala07_Test_java {

    public static void main(String[] args) {
            BBB  bbb = new BBB();
            bbb.test(); //李四， 张三
    }
}
class AAA{
    public String name = "张三";
}
class BBB extends AAA{
    public String name = "李四";
    public void test(){
        System.out.println(this.name);
        System.out.println(super.name);
    }
}
