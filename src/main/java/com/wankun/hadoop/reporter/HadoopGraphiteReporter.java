package com.wankun.hadoop.reporter;

import static com.wankun.hadoop.reporter.Configuration.*;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.graphite.Graphite;
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
    ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();

    /*GraphiteReporter reporter;
    if (usePickled) {
      final PickledGraphite pickledGraphite = new PickledGraphite(new InetSocketAddress(graphiteHost, graphitePort));
      reporter = GraphiteReporter.forRegistry(registry)
          .prefixedWith(graphitePrefix)
          .build(pickledGraphite);
    } else {
      final Graphite graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort));
      reporter = GraphiteReporter.forRegistry(registry)
          .prefixedWith(graphitePrefix)
          .build(graphite);
    }*/

    reporter.start(period, TimeUnit.SECONDS);
  }
}
