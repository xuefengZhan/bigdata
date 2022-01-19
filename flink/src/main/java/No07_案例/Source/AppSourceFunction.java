package No07_案例.Source;


import No07_案例.Bean.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public  class AppSourceFunction implements SourceFunction<MarketingUserBehavior> {

    private boolean flag = true;
    private final List<String> userBehaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
    private final List<String> channelList = Arrays.asList("HUAWEI", "XIAOMI", "OPPO", "VIVO");

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        Random random = new Random();
        while (flag) {
            ctx.collect(
                    new MarketingUserBehavior(
                            random.nextLong(),
                            userBehaviorList.get(random.nextInt(userBehaviorList.size())),
                            channelList.get(random.nextInt(channelList.size())),
                            System.currentTimeMillis()
                    )
            );
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}