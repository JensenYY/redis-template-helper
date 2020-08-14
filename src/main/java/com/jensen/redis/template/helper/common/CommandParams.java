package com.jensen.redis.template.helper.common;

import com.jensen.redis.template.helper.utils.DeserializeUtils;

public enum CommandParams {

  XN(DeserializeUtils.stringToUTF8Bytes("NX")),
  EX(DeserializeUtils.stringToUTF8Bytes("EX"));

  private byte[] value;

  CommandParams(byte[] value) {
    this.value = value;
  }

  public byte[] getValue() {
    return value;
  }
}
