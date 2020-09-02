package com.wankun.hadoop.reporter.collector;

import static com.wankun.hadoop.reporter.Configuration.registry;
import static com.wankun.hadoop.reporter.Configuration.metricsUpdateTime;
import static com.wankun.hadoop.reporter.util.SinkUtil.name;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.codahale.metrics.MetricRegistry;
import com.wankun.hadoop.reporter.util.HttpClientUtil;

import java.io.IOException;
import java.util.Set;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-02.
 */
public abstract class JmxCollector implements Runnable {
  private String url;


  private Set<String> metricNames;

  public abstract String getName();

  public void init(String url) {
    this.url = url;
  }

  public abstract MetricRegistry collect(JSONArray beans);

  @Override
  public void run() {
    try {
      String response = HttpClientUtil.httpGetRequest(url);
      JSONArray beans = JSON.parseObject(response).getJSONArray("beans");

      MetricRegistry collectMetrics = collect(beans);
      // update last fetch time
      long now = System.currentTimeMillis();
      collectMetrics.getGauges().keySet()
          .stream()
          .forEach(metricName -> metricsUpdateTime.put(name(getName(), metricName), now));

      registry.registerAll(getName(), collectMetrics);

      Set<String> newMetricNames = collectMetrics.getNames();
      metricNames.removeAll(newMetricNames);
      metricNames.stream().map(registry::remove);
      metricNames = newMetricNames;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
