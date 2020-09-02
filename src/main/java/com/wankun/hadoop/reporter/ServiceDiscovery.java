package com.wankun.hadoop.reporter;

import static com.wankun.hadoop.reporter.Configuration.*;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.wankun.hadoop.reporter.collector.JmxCollector;
import com.wankun.hadoop.reporter.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-08-31.
 */
public class ServiceDiscovery implements Runnable {

  protected static Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);

  private Map<String, JmxCollector> collectors = Maps.newHashMap();
  private Map<String, Class> collectorMap = new HashMap<>();
  private String filepath;

  private ExecutorService executorService;

  public ServiceDiscovery( MetricRegistry metrics) throws Exception {
    ServiceLoader.load(JmxCollector.class).forEach(collector ->
        collectorMap.put(collector.getName(), collector.getClass())
    );
    if (new File("/etc/cluster.json").exists()) {
      filepath = "/etc/cluster.json";
    } else if (new File("cluster.json").exists()) {
      filepath = "cluster.json";
    } else {
      throw new Exception("Failed to find cluster configuration!");
    }

    executorService = Executors.newFixedThreadPool(collectorPoolSize);
  }

  @Override
  public void run() {
    try {
      JSONObject services = JsonUtil.readJsonObject(filepath);
      services.forEach((service, urlObjs) -> {
        ((JSONArray) urlObjs).forEach(urlObj -> {
          String url = urlObj.toString();

          try {
            logger.info("add new Collector{}", url);
            Class<JmxCollector> collectorClass = collectorMap.get(service);
            if (collectorClass != null) {
              JmxCollector newCollector = collectorClass.newInstance();
              newCollector.init(url);
              executorService.submit(newCollector);
            }

          } catch (Exception e) {
            logger.error("init collector instance error!", e);
          }
        });
      });
      logger.debug("collectors : {}", collectors);
    } catch (IOException e) {
      logger.error("Service discovery failed!");
    }

    // remove death metrics
    long deathTime = System.currentTimeMillis() - period * 1000 * 5;
    metricsUpdateTime.entrySet().removeIf(en -> en.getValue() < deathTime);
  }
}
