package com.code.ly.flink.stream.cases.deduplicate;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  去重方案1：布隆过滤器
 *  数据流 Tuple3<Integer, Long, String>  页面id  订单id  订单信息
 */
public class deduplicatev1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream stream = env.socketTextStream("",8080).map(new MapFunction<String, Tuple3<Integer, Long, String>>() {
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
        })
                .process(new OrderDeduplicateProcessFunc(), TypeInformation.of(String.class))
                .name("page-order-process")
                .uid("page-order-process");
    }

    // 去重用的ProcessFunction
    public static final class OrderDeduplicateProcessFunc extends KeyedProcessFunction<Integer, Tuple3<Integer, Long, String>, String> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOGGER = LoggerFactory.getLogger(OrderDeduplicateProcessFunc.class);
        private static final int BF_CARDINAL_THRESHOLD = 1000000;
        private static final double BF_FALSE_POSITIVE_RATE = 0.01;
        private volatile BloomFilter<Long> orderFilter;

        /**
         * 初始化时创建过滤器
         * @param parameters
         * @throws Exception
         */
        public void open(Configuration parameters) throws Exception {
            long s = System.currentTimeMillis();
            orderFilter = BloomFilter.create(Funnels.longFunnel(), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE);
            long e = System.currentTimeMillis();
            LOGGER.info("Created Guava BloomFilter, time cost: " + (e - s));
        }

        @Override
        public void processElement(Tuple3<Integer, Long, String> value, Context context, Collector<String> collector) throws Exception {
            long orderId = value.f1;
            if (!orderFilter.mightContain(orderId)) {
                orderFilter.put(orderId);
                collector.collect(value.f2);
            }
            context.timerService().registerProcessingTimeTimer(tomorrowZeroTimestampMs(System.currentTimeMillis(), 8) + 1);
        }

        /**
         *  每天0时0分0秒定时重建过滤器
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long s = System.currentTimeMillis();
            orderFilter = BloomFilter.create(Funnels.longFunnel(), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE);
            long e = System.currentTimeMillis();
            LOGGER.info("Timer triggered & resetted Guava BloomFilter, time cost: " + (e - s));
        }

        @Override
        public void close() throws Exception {
            orderFilter = null;
        }
    }

    // 根据当前时间戳获取第二天0时0分0秒的时间戳
    public static long tomorrowZeroTimestampMs(long now, int timeZone) {
        return now - (now + timeZone * 3600000) % 86400000 + 86400000;
    }

}
