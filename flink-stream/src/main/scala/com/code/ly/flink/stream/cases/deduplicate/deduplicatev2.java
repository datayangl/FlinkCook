package com.code.ly.flink.stream.cases.deduplicate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  去重方案2：a 按照页面id 做keyby，然后使用mapstate 记录订单id是否存在
 *             b 按照订单id 做keyby，然后直接使用ValueState<Boolean> 记录是否存在
 *  数据流 Tuple3<Integer, Long, String>  页面id  订单id  订单信息
 */
public class deduplicatev2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream stream = env.socketTextStream("", 8080).map(new MapFunction<String, Tuple3<Integer, Long, String>>() {
            @Override
            public Tuple3<Integer, Long, String> map(String s) throws Exception {
                // 具体数据提取省略
                return null;
            }
        }).keyBy(new KeySelector<Tuple3<Integer, Long, String>, Integer>() {
            @Override
            public Integer getKey(Tuple3<Integer, Long, String> in) throws Exception {
                return in.f0;
            }
        }).process(new OrderDeduplicateProcessFunc());
    }


    public static final class OrderDeduplicateProcessFunc extends KeyedProcessFunction<Integer, Tuple3<Integer, Long, String>, String> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOGGER = LoggerFactory.getLogger(deduplicatev1.OrderDeduplicateProcessFunc.class);
        private ValueState<Boolean> existState;

        public void open(Configuration parameters)  {
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .cleanupInRocksdbCompactFilter(10000)
                    .build();

            ValueStateDescriptor<Boolean> existStateDesc = new ValueStateDescriptor<>(
                    "",
                    Boolean.class
            );
            existStateDesc.enableTimeToLive(stateTtlConfig);

            existState = this.getRuntimeContext().getState(existStateDesc);
        }

        @Override
        public void processElement(Tuple3<Integer, Long, String> value, Context context, Collector<String> collector) throws Exception {
            if (existState.value() == null) {
                existState.update(true);
                collector.collect(value.f2);
            }
        }
    }
}
