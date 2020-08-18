package com.jensen.redis.template.helper.common;

public enum CommandParams {

  XN("NX"),
  EX("EX");

  private String value;

  CommandParams(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
