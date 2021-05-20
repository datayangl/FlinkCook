package com.code.ly.flink.sql.source;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.reflections.Reflections.log;

/**
 * @author
 * @link see https://github.com/lonelyGhostisdog/flinksql/blob/master/src/main/java/lookup/RedisLookupFunction.java
 */
public class RedisLookupFunction extends AsyncTableFunction<Row> {
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String ip;
    private final int port;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private transient Cache<String, String> cache;

    /**
     * 异步客户端及命令
     */
    private transient RedisAsyncCommands<String, String> asyncClient;
    private transient RedisClient redisClient;

    public RedisLookupFunction(String[] fieldNames, TypeInformation[] fieldTypes, String ip, int port, long cacheMaxSize, long cacheExpireMs) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.ip = ip;
        this.port = port;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    /**
     * 整个逻辑是优先从缓存查询，取的到就返回，取不到就从redis异步查询
     * @param future
     * @param key
     */
    public void eval(CompletableFuture<Collection<Row>> future, String key) {
        if (cache != null) {
            String value = cache.getIfPresent(key);
            log.info("value in cache is null?:{}", value == null);
            if (value != null) {
                future.complete(Collections.singletonList(Row.of(key, value)));
                return;
            }
        }

        try {
            asyncClient.get(key).thenAccept(value -> {
                if (value == null) {
                    value = "";
                }
                if (cache != null) {
                    cache.put(key, value);
                }
                future.complete(Collections.singletonList(Row.of(key, value)));
            });
        } catch (Exception e) {
            log.error("search redis failed", e);
            throw new RuntimeException("search redis failed", e);
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            RedisURI redisUri = RedisURI.builder()
                    .withHost(ip)
                    .withPort(port)
                    .build();
            redisClient = RedisClient.create(redisUri);
            asyncClient = redisClient.connect().async();
        } catch (Exception e) {
            throw new Exception("build redis async client failed", e);
        }

        try {
            /**
             * 初始化缓存
             */
            this.cache = cacheMaxSize <= 0 || cacheExpireMs <= 0 ? null : CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
            log.info("cache is null ? :{}", cache == null);
        } catch (Exception e) {
            throw new Exception("build cache failed", e);
        }

    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void close() throws Exception {
        cache.cleanUp();
        redisClient.shutdown();
        super.close();
    }

    /**
     * builder 设计模式
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private String ip;
        private int port;

        private long cacheMaxSize;
        private long cacheExpireMs;


        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }


        public RedisLookupFunction build() {
            return new RedisLookupFunction(fieldNames, fieldTypes, ip, port, cacheMaxSize, cacheExpireMs);
        }
    }
}
