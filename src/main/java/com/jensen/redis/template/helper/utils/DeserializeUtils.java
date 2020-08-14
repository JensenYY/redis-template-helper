package com.jensen.redis.template.helper.utils;

import com.google.common.collect.Maps;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import org.springframework.util.CollectionUtils;

public class DeserializeUtils {

  private DeserializeUtils() {
  }

  public static Map<String, String> deserializeMap(Map<byte[], byte[]> map) {
    if (CollectionUtils.isEmpty(map)) {
      return Collections.emptyMap();
    }

    Map<String, String> resultMap = Maps.newHashMap();
    for (Entry<byte[], byte[]> entry : map.entrySet()) {
      resultMap.put(UTF8BytesToString(entry.getKey()), UTF8BytesToString(entry.getValue()));
    }

    return resultMap;
  }

  public static byte[] stringToUTF8Bytes(String str) {
    return str.getBytes(StandardCharsets.UTF_8);
  }

  public static String UTF8BytesToString(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
