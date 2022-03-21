package common;

public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    //Phoneix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoneix连接参数
    public static final String PHOENIX_SERVER =  "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}
