package scala.chapter02;

public class Scala04_DataType3 {

    public static void main(String[] args) {
        A a = new B();
        test(a);
    }

    public static void test(A a){
        System.out.println("aaa");
    }

    public static void test(B b){
        System.out.println("bbb");
    }
}


class A{

}
class B extends A{}