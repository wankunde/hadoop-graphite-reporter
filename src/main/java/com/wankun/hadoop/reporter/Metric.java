package com.wankun.hadoop.reporter;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-03.
 */
public class Metric {

  public Metric(String name, Number value) {
    this(name, value, System.currentTimeMillis() / 1000);
  }

  public Metric(String name, Number value, Long ts) {
    this.name = name;
    this.value = value;
    this.ts = ts;
  }

  private String name;
  private Number value;
  private Long ts;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Number getValue() {
    return value;
  }

  public void setValue(Number value) {
    this.value = value;
  }

  public Long getTs() {
    return ts;
  }

  public void setTs(Long ts) {
    this.ts = ts;
  }

  @Override
  public String toString() {
    return name + " " + value + " " + ts + "%n";
  }
}
