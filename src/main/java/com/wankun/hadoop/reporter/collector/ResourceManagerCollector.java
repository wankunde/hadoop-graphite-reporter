package com.wankun.hadoop.reporter.collector;

import static com.wankun.hadoop.reporter.util.SinkUtil.name;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.MetricRegistry;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-01.
 */
public class ResourceManagerCollector extends JmxCollector {

  @Override
  public String getName() {
    return "RESOURCEMANAGER";
  }

  @Override
  public MetricRegistry collect(JSONArray beans) {
    MetricRegistry metrics = new MetricRegistry();
    for (int i = 0; i < beans.size(); i++) {
      JSONObject bean = beans.getJSONObject(i);
      String name = bean.getString("name");
      String host = bean.getString("tag.Hostname");
      if (name.startsWith("Hadoop:service=ResourceManager,name=QueueMetrics")) {
        String queue = bean.getString("tag.Queue");
        String[] keys = {
            "running_0",
            "running_60",
            "running_300",
            "running_1440",
            "FairShareMB",
            "FairShareVCores",
            "SteadyFairShareMB",
            "SteadyFairShareVCores",
            "MinShareMB",
            "MinShareVCores",
            "MaxShareMB",
            "MaxShareVCores",
            "MaxApps",
            "MaxAMShareMB",
            "MaxAMShareVCores",
            "AmResourceUsageMB",
            "AmResourceUsageVCores",
            "AppsSubmitted",
            "AppsRunning",
            "AppsPending",
            "AppsCompleted",
            "AppsKilled",
            "AppsFailed",
            "AllocatedMB",
            "AllocatedVCores",
            "AllocatedContainers",
            "AggregateContainersAllocated",
            "AggregateContainersReleased",
            "AvailableMB",
            "AggregateContainersPreempted",
            "AvailableVCores",
            "PendingMB",
            "PendingVCores",
            "PendingContainers",
            "ReservedMB",
            "ReservedVCores",
            "ReservedContainers",
            "ActiveUsers",
            "ActiveApplications",
            "AppAttemptFirstContainerAllocationDelayNumOps",
            "AppAttemptFirstContainerAllocationDelayAvgTime"
        };
        if (bean.get("tag.User") == null) {
          for (String key : keys) {
            metrics.gauge(name("QueueMetrics", queue, host, key), () -> () -> bean.get(key));
          }
        }
      } else if (name.startsWith("Hadoop:service=ResourceManager,name=RpcActivity")) {
        String port = bean.getString("tag.port");
        String[] keys = {
            "ReceivedBytes",
            "SentBytes",
            "RpcQueueTimeNumOps",
            "RpcQueueTimeAvgTime",
            "RpcProcessingTimeNumOps",
            "RpcProcessingTimeAvgTime",
            "RpcAuthenticationFailures",
            "RpcAuthenticationSuccesses",
            "RpcAuthorizationFailures",
            "RpcAuthorizationSuccesses",
            "RpcSlowCalls",
            "RpcClientBackoff",
            "NumOpenConnections",
            "CallQueueLength",
            "NumDroppedConnections"
        };
        for (String key : keys) {
          metrics.gauge(name("RpcActivity", host, port, key), () -> () -> bean.get(key));
        }
      }
    }
    return metrics;
  }
}
