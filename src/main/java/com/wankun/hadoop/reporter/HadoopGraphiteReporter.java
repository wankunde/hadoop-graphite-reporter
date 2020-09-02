package com.wankun.hadoop.reporter;

import static com.wankun.hadoop.reporter.Configuration.*;

import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.PickledGraphite;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-02.
 */
public class HadoopGraphiteReporter {

  public static void main(String[] args) throws Exception {
    Executors.newSingleThreadScheduledExecutor()
        .scheduleWithFixedDelay(new ServiceDiscovery(registry), 0, period, TimeUnit.SECONDS);

    // for debug
//    ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).build();

    final PickledGraphite pickledGraphite = new PickledGraphite(new InetSocketAddress(graphiteHost, graphitePort));
    pickledGraphite.flush();
    final GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
        .prefixedWith(graphitePrefix)
        .build(pickledGraphite);

    reporter.start(period, TimeUnit.SECONDS);
  }
}
