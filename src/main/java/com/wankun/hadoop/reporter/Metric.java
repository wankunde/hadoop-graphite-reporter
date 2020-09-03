package com.wankun.hadoop.reporter;

import com.google.common.base.Strings;

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

  public static String name(String name, String... names) {
    final StringBuilder builder = new StringBuilder();
    if (!Strings.isNullOrEmpty(Configuration.graphitePrefix)) {
      append(builder, Configuration.graphitePrefix);
    }
    append(builder, name);
    if (names != null) {
      for (String s : names) {
        append(builder, s);
      }
    }
    return builder.toString();
  }

  private static void append(StringBuilder builder, String part) {
    if (part != null && !part.isEmpty()) {
      if (builder.length() > 0) {
        builder.append('.');
      }

      while (part.indexOf('.') >= 0) {
        part = part.replace('.', '-');
      }
      builder.append(part);
    }
  }
}
