package scala.chapter03;

public class Scala02_Oper_java {
    public static void main(String[] args) {
        int i = 0;
        int j = i++; //tmp=(i++) = 0;  j = tmp = 0;
        i = i++;  // tmp = (i++) = 0 ; i = tmp = 0;  最后i++
        System.out.println(i);//0


        //= 的作用是将等号右边的计算结果赋值给等号的左边
        //
    }
}
