package Util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    //T是每行记录  List 表示多行记录
    //参数Class 是将封装的类型给传进来
    //是否将驼峰转为java命名方式
    public static <T> List<T> queryList(Connection connection, String sql,Class<T> Clz,Boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        //结果集
        ArrayList<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //获取结果集的元数据 根据列数来赋值
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while(resultSet.next()){
            //每一行都new一个对象
            T t = Clz.newInstance();

            for (int i = 1; i <= columnCount; i++) {
                //获取列名  如果需要 将下划线命名方式转为驼峰
                String columnName = metaData.getColumnName(i);

                if(underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE
                            .to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }
                //获取列值
                Object colValue = resultSet.getObject(i);

                //给对象赋值 t是反射获取的
                BeanUtils.setProperty(t,columnName,colValue);
            }
            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();

        return resultList;

    }
}
