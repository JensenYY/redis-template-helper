package com.jensen.redis.template.helper;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

public class TestRedisTemplateHelper {

  private static RedisTemplateHelper redisTemplateHelper;

  @BeforeClass
  public static void init() {
    RedisStandaloneConfiguration rsc = new RedisStandaloneConfiguration();
    rsc.setPort(6379);
    rsc.setPassword(RedisPassword.of("test"));
    rsc.setHostName("10.211.55.3");

    RedisConnectionFactory fac = new JedisConnectionFactory(rsc);

    redisTemplateHelper = new RedisTemplateHelper(fac);
  }

  @Test
  public void testSetnx() {
    try {
      String key = "redis:helper:test:setnx";
      String value = Instant.now().toString();
      redisTemplateHelper.delete(key);
      boolean result = redisTemplateHelper.setnx(key, value, 100);
      Assert.assertTrue(result);
      String redisValue = redisTemplateHelper.get(key);
      Assert.assertEquals(value, redisValue);
      redisTemplateHelper.delete(key);
      boolean isExist = redisTemplateHelper.exist(key);
      Assert.assertFalse(isExist);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetex() {
    try {
      String key = "redis:helper:test:setex";
      String value = Instant.now().toString();
      redisTemplateHelper.delete(key);
      redisTemplateHelper.setex(key, value, 10);
      String redisValue = redisTemplateHelper.get(key);
      Assert.assertEquals(value, redisValue);
      TimeUnit.SECONDS.sleep(10);
      boolean isExist = redisTemplateHelper.exist(key);
      Assert.assertFalse(isExist);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSet() {
    try {
      String key = "redis:helper:test:set";
      String value = Instant.now().toString();
      redisTemplateHelper.delete(key);
      redisTemplateHelper.set(key, value);
      String redisValue = redisTemplateHelper.get(key);
      Assert.assertEquals(value, redisValue);
      redisTemplateHelper.expiry(key, 10);
      TimeUnit.SECONDS.sleep(10);
      boolean isExist = redisTemplateHelper.exist(key);
      Assert.assertFalse(isExist);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testList() {
    try {
      String key = "redis:helper:test:list";
      String[] values = {"a", "b", "c", "d", "e"};
      redisTemplateHelper.delete(key);
      redisTemplateHelper.rpush(key, values);
      List<String> redisValues = redisTemplateHelper.lrange(key, 0, -1);
      Assert.assertArrayEquals(values, redisValues.toArray(new String[]{}));
      redisTemplateHelper.delete(key);
      redisTemplateHelper.lpush(key, values);
      redisValues = redisTemplateHelper.lrange(key, 0, -1);
      Collections.reverse(redisValues);
      Assert.assertArrayEquals(values, redisValues.toArray(new String[]{}));
      String lValue = redisTemplateHelper.lpop(key);
      Assert.assertEquals("e", lValue);
      String rValue = redisTemplateHelper.rpop(key);
      Assert.assertEquals("a", rValue);
      long length = redisTemplateHelper.llen(key);
      Assert.assertEquals(3L, length);
      redisTemplateHelper.lpush(key, "e");
      redisTemplateHelper.rpush(key, "a");
      redisValues = redisTemplateHelper.lrange(key, 0, -1);
      Collections.reverse(redisValues);
      Assert.assertArrayEquals(values, redisValues.toArray(new String[]{}));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testHash() {
    try {
      String key = "redis:helper:test:hash";
      Map<String, String> values = Maps.newHashMap();
      values.put("1", "a");
      values.put("2", "b");
      values.put("3", "c");
      redisTemplateHelper.delete(key);
      redisTemplateHelper.hset(key, "1", "a");
      redisTemplateHelper.hset(key, "2", "b");
      redisTemplateHelper.hset(key, "3", "c");
      Map<String, String> map = redisTemplateHelper.hgetall(key);
      Assert.assertEquals(values, map);
      String fieldValue = redisTemplateHelper.hget(key, "1");
      Assert.assertEquals("a", fieldValue);
      redisTemplateHelper.hdel(key, "1");
      fieldValue = redisTemplateHelper.hget(key, "1");
      Assert.assertNull(fieldValue);
      redisTemplateHelper.hdel(key, "2");
      fieldValue = redisTemplateHelper.hget(key, "2");
      Assert.assertNull(fieldValue);
      redisTemplateHelper.hmset(key, values);
      map = redisTemplateHelper.hgetall(key);
      Assert.assertEquals(values, map);
      long length = redisTemplateHelper.hlen(key);
      Assert.assertEquals(3L, length);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testIncrDecr() {
    try {
      String key = "redis:helper:test:incrdecr";
      redisTemplateHelper.delete(key);
      long value = redisTemplateHelper.incr(key);
      Assert.assertEquals(1L, value);
      value = redisTemplateHelper.decr(key);
      Assert.assertEquals(0L, value);
      value = redisTemplateHelper.incrBy(key, 10);
      Assert.assertEquals(10L, value);
      value = redisTemplateHelper.decrBy(key, 10);
      Assert.assertEquals(0L, value);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
