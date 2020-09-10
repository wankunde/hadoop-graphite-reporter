package com.wankun.hadoop.reporter.collector;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wankun.hadoop.reporter.Metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  public List<Metric> collect(JSONArray beans) {
    List<Metric> metrics = new ArrayList<>();
    for (int i = 0; i < beans.size(); i++) {
      JSONObject bean = beans.getJSONObject(i);
      String name = bean.getString("name");
      // a trick method to filter standby node
      if ("Hadoop:service=ResourceManager,name=QueueMetrics,q0=root".equals(name)
          && bean.getInteger("running_0") == 0
          && bean.getInteger("running_60") == 0
          && bean.getInteger("running_300") == 0
          && bean.getInteger("running_1440") == 0) {
        return Collections.EMPTY_LIST;
      }

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
        if(queue.startsWith("root.users.")) {
          keys = new String[]{
              "AppsSubmitted",
              "AppsRunning",
              "AppsPending",
              "AppsCompleted",
              "AppsKilled",
              "AppsFailed",
              "AllocatedMB",
              "AllocatedVCores",
              "AllocatedContainers",
              "AvailableMB",
              "AvailableVCores",
              "PendingMB",
              "PendingVCores",
              "PendingContainers",
              "ReservedMB",
              "ReservedVCores",
              "ReservedContainers"
          };
        }
        if (bean.get("tag.User") == null) {
          for (String key : keys) {
            Object value = bean.get(key);
            if(value!=null){
              metrics.add(new Metric(name("QueueMetrics", queue, host, key), (Number)value));
            }
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
          Object value = bean.get(key);
          if(value!=null){
            metrics.add(new Metric(name("RpcActivity", host, port, key), (Number) value));
          }
        }
      }
    }
    return metrics;
  }
}
