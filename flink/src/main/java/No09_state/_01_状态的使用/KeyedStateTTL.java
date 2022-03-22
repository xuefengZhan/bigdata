package No09_state._01_状态的使用;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

public class KeyedStateTTL {
    public static void main(String[] args) {

        //KeyedState TTL设置
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);

        stateDescriptor.enableTimeToLive(ttlConfig);
    }
}
