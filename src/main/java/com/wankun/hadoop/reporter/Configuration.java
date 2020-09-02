package com.wankun.hadoop.reporter;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-02.
 */
public class Configuration {

  private static <T> Object getConfig(String name, T defaultValue) {
    String env = System.getenv(name);
    if (env == null) {
      return defaultValue;
    } else if (defaultValue instanceof Integer) {
      return Integer.parseInt(env);
    } else if (defaultValue instanceof Long) {
      return Long.parseLong(env);
    } else if (defaultValue instanceof Short) {
      return Short.parseShort(env);
    } else if (defaultValue instanceof Double) {
      return Double.parseDouble(env);
    } else if (defaultValue instanceof Float) {
      return Float.parseFloat(env);
    } else if (defaultValue instanceof Boolean) {
      return Boolean.parseBoolean(env);
    } else
      return env;
  }

  public static String get(String name, String defaultValue) {
    return (String) getConfig(name, defaultValue);
  }

  public static Integer getInteger(String name, Integer defaultValue) {
    return (Integer) getConfig(name, defaultValue);
  }

  public static Long getLong(String name, Long defaultValue) {
    return (Long) getConfig(name, defaultValue);
  }

  public static Short getShort(String name, Short defaultValue) {
    return (Short) getConfig(name, defaultValue);
  }

  public static Double getDouble(String name, Double defaultValue) {
    return (Double) getConfig(name, defaultValue);
  }

  public static Float getFloat(String name, Float defaultValue) {
    return (Float) getConfig(name, defaultValue);
  }

  public static Boolean getBoolean(String name, Boolean defaultValue) {
    return (Boolean) getConfig(name, defaultValue);
  }

  public static int defaultPort() {
    if (getBoolean("USE_PICKLED", false))
      return 2004;
    else
      return 2003;
  }

  public static boolean usePickled = getBoolean("USE_PICKLED", false);
  public static String graphiteHost = get("GRAPHITE_HOST", "");
  public static Integer graphitePort = getInteger("GRAPHITE_PORT", defaultPort());
  public static String graphitePrefix = get("GRAPHITE_PREFIX", "hadoop");

  public static long period = getLong("PERIOD", 15L);
  public static int collectorPoolSize = getInteger("COLLECTOR_POOL_SIZE", 50);

  public static MetricRegistry registry = new MetricRegistry();
  public static Map<String, Long> metricsUpdateTime = Maps.newConcurrentMap();
}
