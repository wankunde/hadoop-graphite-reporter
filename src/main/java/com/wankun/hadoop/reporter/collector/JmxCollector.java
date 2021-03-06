package com.wankun.hadoop.reporter.collector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.wankun.hadoop.reporter.Configuration;
import com.wankun.hadoop.reporter.Metric;
import com.wankun.hadoop.reporter.util.HttpClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-02.
 */
public abstract class JmxCollector implements Runnable {

  protected static Logger logger = LoggerFactory.getLogger(JmxCollector.class);

  private String url;

  public abstract String getName();

  public void init(String url) {
    this.url = url;
  }

  public abstract List<Metric> collect(JSONArray beans);

  public void writeMetrics(Metric... metrics) throws IOException {
    writeMetrics(Arrays.asList(metrics), false);
  }

  public void writeMetrics(List<Metric> metrics, boolean filterZero) throws IOException {
    if (metrics != null && metrics.size() > 0) {
      AtomicInteger num = new AtomicInteger();
      Socket socket = new Socket(Configuration.graphiteHost, Configuration.graphitePort);
      PrintWriter writer = new PrintWriter(socket.getOutputStream(), false);
      metrics.stream().filter(metric -> !filterZero || metric.getValue().doubleValue() != 0)
              .forEach(metric -> {
                writer.printf(metric.toString());
                if (num.incrementAndGet() % 200 == 0) {
                  writer.flush();
                }
              });
      writer.flush();
      writer.close();
      socket.close();
    }
  }

  @Override
  public void run() {
    try {
      URL urlBean = new URL(url);
      String host = urlBean.getHost();
      String port = Integer.toString(urlBean.getPort());
      Stopwatch stopwatch = Stopwatch.createStarted();
      String response = HttpClientUtil.httpGetRequest(url);
      long httpTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      JSONArray beans = JSON.parseObject(response).getJSONArray("beans");
      List<Metric> metrics = collect(beans);
      long parseTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      writeMetrics(metrics, true);
      long sinkTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      writeMetrics(
          new Metric(name(host, port, "httpTime"), httpTime),
          new Metric(name(host, port, "parseTime"), parseTime - httpTime),
          new Metric(name(host, port, "sinkTime"), sinkTime - parseTime),
          new Metric(name(host, port, "metricsSize"), metrics.size()));
    } catch (Exception e) {
      logger.error("failed to collect metrics.", e);
    }
  }

  public String name(String name, String... names) {
    final StringBuilder builder = new StringBuilder();
    if (!Strings.isNullOrEmpty(Configuration.graphitePrefix)) {
      append(builder, Configuration.graphitePrefix);
    }
    append(builder, getName());
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
