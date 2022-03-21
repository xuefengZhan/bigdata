package Function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimAsyncJoinFunction<T> {
   String getKey(T input);
    void join(T t, JSONObject jsonObject) throws ParseException;

}
