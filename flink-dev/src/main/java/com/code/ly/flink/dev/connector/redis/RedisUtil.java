package com.code.ly.flink.dev.connector.redis;

import com.getui.axe.v3.codis.common.JodisFactory;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

public class RedisUtil implements Serializable {
    private String codisZk;
    private String codisZkProxydir;

    private volatile boolean init = false;
    private static Jedis client = null;

    public RedisUtil(String codisZk, String codisZkProxydir) {
        this.codisZk = codisZk;
        this.codisZkProxydir = codisZkProxydir;
    }

    public Jedis getJedis() {
        if (client != null) {
            return client;
        } else {
            synchronized (this) {
                if(client  == null) {
                    client = JodisFactory.getCodisPool(codisZk, codisZkProxydir, 10).getResource();
                }
            }
        }

        return client;
    }


    public static Jedis getJedis(String codisZk, String codisZkProxydir) {
        Jedis client = JodisFactory.getCodisPool(codisZk, codisZkProxydir, 10).getResource();
        return client;
    }

    public void init() {
        client = getJedis();
    }
}

