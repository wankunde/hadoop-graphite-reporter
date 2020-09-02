package com.wankun.hadoop.reporter.util;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-02.
 */
public class SinkUtil {

  public static String name(String name, String... names) {
    final StringBuilder builder = new StringBuilder();
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
