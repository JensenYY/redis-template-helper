package com.jensen.redis.template.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jensen.redis.template.helper.common.Command;
import com.jensen.redis.template.helper.common.CommandParams;
import com.jensen.redis.template.helper.utils.DeserializeUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.util.CollectionUtils;

public class RedisTemplateHelper {

  private RedisTemplate<String, String> redisTemplate;

  public RedisTemplateHelper(RedisConnectionFactory redisConnectionFactory) {
    redisTemplate = new RedisTemplate<>();
    redisTemplate.setConnectionFactory(redisConnectionFactory);
  }

  public void delete(String key) {
    redisTemplate.delete(key);
  }

  public Boolean exist(String key) {
    return redisTemplate.hasKey(key);
  }

  public String get(String key) {
    return redisTemplate.opsForValue().get(key);
  }

  public void set(String key, String value) {
    redisTemplate.opsForValue().set(key, value);
  }

  public void setex(String key, String value, long expiry) {
    redisTemplate.opsForValue().set(key, value, expiry, TimeUnit.SECONDS);
  }

  public Long setnx(String key, String value, long expiry) {
    return redisTemplate.execute(new RedisCallback<Long>() {
      @Override
      public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
        Object result = redisConnection
            .execute(Command.SET.getValue(), DeserializeUtils.stringToUTF8Bytes(key),
                DeserializeUtils.stringToUTF8Bytes(value), CommandParams.XN.getValue(),
                CommandParams.EX.getValue(),
                DeserializeUtils.stringToUTF8Bytes(String.valueOf(expiry)));
        return Objects.isNull(result) ? 0L : (Long) result;
      }
    });
  }

  public void expiry(String key, long expiry) {
    redisTemplate.expire(key, expiry, TimeUnit.SECONDS);
  }

  public List<String> lrange(String key, long start, long end) {
    return redisTemplate.opsForList().range(key, start, end);
  }

  public Long lpush(String key, String value) {
    return redisTemplate.opsForList().leftPush(key, value);
  }

  public Long lpush(String key, String... values) {
    return redisTemplate.opsForList().leftPushAll(key, values);
  }

  public Long lpush(String key, List<String> values) {
    return redisTemplate.opsForList().leftPushAll(key, values);
  }

  public Long rpush(String key, String value) {
    return redisTemplate.opsForList().rightPush(key, value);
  }

  public Long rpush(String key, String... values) {
    return redisTemplate.opsForList().rightPushAll(key, values);
  }

  public Long rpush(String key, List<String> values) {
    return redisTemplate.opsForList().rightPushAll(key, values);
  }

  public String lpop(String key) {
    return redisTemplate.opsForList().leftPop(key);
  }

  public String rpop(String key) {
    return redisTemplate.opsForList().rightPop(key);
  }

  public Long llen(String key) {
    return redisTemplate.opsForList().size(key);
  }

  public Long hdel(String key, String... values) {
    return redisTemplate.opsForHash().delete(key, (Object[]) values);
  }

  public Long hdel(String key, List<String> values) {
    return redisTemplate.opsForHash().delete(key, values.toArray());
  }

  public String hget(String key, String field) {
    Object result = redisTemplate.opsForHash().get(key, field);
    return Objects.isNull(result) ? null : (String) result;
  }

  public void hset(String key, String field, String value) {
    redisTemplate.opsForHash().put(key, field, value);
  }

  public List<String> hmget(String key, String... fields) {
    List<Object> result = redisTemplate.opsForHash().multiGet(key, Lists.newArrayList(fields));
    return CollectionUtils.isEmpty(result) ? Collections.emptyList()
        : result.stream().map(String::valueOf).collect(Collectors.toList());
  }

  public void hmset(String key, Map<String, String> hash) {
    redisTemplate.opsForHash().putAll(key, hash);
  }

  public Boolean hexists(String key, String field) {
    return redisTemplate.opsForHash().hasKey(key, field);
  }

  public Long hlen(String key) {
    return redisTemplate.opsForHash().size(key);
  }

  public Map<String, String> hgetall(String key) {
    return redisTemplate.execute(new RedisCallback<Map<String, String>>() {
      @Override
      public Map<String, String> doInRedis(RedisConnection connection) throws DataAccessException {
        Map<byte[], byte[]> entries = connection.hGetAll(key.getBytes());
        return CollectionUtils.isEmpty(entries) ? Collections.emptyMap()
            : DeserializeUtils.deserializeMap(entries);
      }
    });
  }

  public Long sadd(String key, String... value) {
    return redisTemplate.opsForSet().add(key, value);
  }

  public Long sadd(String key, List<String> values) {
    return redisTemplate.opsForSet().add(key, values.toArray(new String[0]));
  }

  public Long srem(String key, String... value) {
    return redisTemplate.opsForSet().remove(key, (Object[]) value);
  }

  public Long srem(String key, List<String> values) {
    return redisTemplate.opsForSet().remove(key, values.toArray());
  }

  public Set<String> smembers(String key) {
    return redisTemplate.opsForSet().members(key);
  }

  public String spop(String key) {
    return redisTemplate.opsForSet().pop(key);
  }

  public List<String> spop(String key, long count) {
    return redisTemplate.opsForSet().pop(key, count);
  }

  public Boolean sismember(String key, String value) {
    return redisTemplate.opsForSet().isMember(key, value);
  }

  public Boolean zadd(String key, double score, String member) {
    return redisTemplate.opsForZSet().add(key, member, score);
  }

  public Long zadd(String key, Map<String, Double> scoreMembers) {
    Set<TypedTuple<String>> members = Sets.newHashSet();

    for (Entry<String, Double> entry : scoreMembers.entrySet()) {
      DefaultTypedTuple<String> item = new DefaultTypedTuple<>(entry.getKey(), entry.getValue());
      members.add(item);
    }

    return redisTemplate.opsForZSet().add(key, members);
  }

  public Set<String> zrange(String key, long start, long end) {
    return redisTemplate.opsForZSet().range(key, start, end);
  }

  public Long zrem(String key, String... members) {
    return redisTemplate.opsForZSet().remove(key, (Object[]) members);
  }

  public Long zrem(String key, List<String> members) {
    return redisTemplate.opsForZSet().remove(key, members.toArray());
  }

  public Long zcount(String key, double min, double max) {
    return redisTemplate.opsForZSet().count(key, min, max);
  }

  public Long zcount(String key, String min, String max) {
    return redisTemplate.opsForZSet().count(key, Double.parseDouble(min), Double.parseDouble(max));
  }

  public Double zincrby(String key, double score, String member) {
    return redisTemplate.opsForZSet().incrementScore(key, member, score);
  }

  public Long zrank(String key, String member) {
    return redisTemplate.opsForZSet().rank(key, member);
  }

  public Set<String> zrangeByScore(String key, double min, double max) {
    return redisTemplate.opsForZSet().rangeByScore(key, min, max);
  }

  public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
    return redisTemplate.opsForZSet().rangeByScore(key, min, max, offset, count);
  }
}
