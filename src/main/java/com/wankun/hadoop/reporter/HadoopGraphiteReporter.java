package com.wankun.hadoop.reporter;

import static com.wankun.hadoop.reporter.Configuration.collectorPoolSize;
import static com.wankun.hadoop.reporter.Configuration.period;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
import java.util.concurrent.TimeUnit;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-02.
 */
public class HadoopGraphiteReporter implements Runnable {

  protected static Logger logger = LoggerFactory.getLogger(HadoopGraphiteReporter.class);

  private static Map<String, Class> collectorMap = new HashMap<>();
  private static String filepath;
  private static ExecutorService executorService;

  public static void main(String[] args) throws Exception {
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

    Executors.newSingleThreadScheduledExecutor()
        .scheduleWithFixedDelay(new HadoopGraphiteReporter(), 0, period, TimeUnit.SECONDS);
  }

  @Override
  public void run() {
    try {
      JSONObject services = JsonUtil.readJsonObject(filepath);
      services.forEach((service, urlObjs) -> {
        ((JSONArray) urlObjs).forEach(urlObj -> {
          String url = urlObj.toString();

          try {
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
    } catch (IOException e) {
      logger.error("Service discovery failed!");
    }
  }
}
