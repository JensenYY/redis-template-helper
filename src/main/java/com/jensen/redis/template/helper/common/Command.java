package com.jensen.redis.template.helper.common;

public enum Command {

  SET("set");

  private String value;

  Command(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}

