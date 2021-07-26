package com.code.ly.flink.dev.demo;

import com.code.ly.flink.dev.connector.redis.RedisUtil;
import com.getui.axe.gid.service.GidBaseService;
import com.getui.axe.v3.thrift.gid.DeviceType;
import redis.clients.jedis.Jedis;

public class JedisDemo {
    public static void main(String[] args) {
        Jedis client = RedisUtil.getJedis("192.168.10.36:2181","/zk/codis/db_codis-demo/proxy");


        GidBaseService service = new GidBaseService();
        String cid = "000000bc9da476f7db49f10aa58d0cb6";
        byte[] key = service.getId2GidKey(cid, DeviceType.cid);
        byte[] value = client.get(key);
        System.out.println(new String(value));
    }
}
