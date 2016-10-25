package com.dataartisans.cookbook.multiaggregate;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class HostMetricGenerator implements SourceFunction<HostMetric> {
  private volatile boolean isRunning = true;

  private String[] hostnames = {
    "vader",
    "luke",
    "hans",
    "leia",
    "r2-d2",
    "c-3po",
    "obi-wan",
    "lando",
    "yoda",
    "chewbacca"
  };

  private String[] domains = {
    "wikipedia.org",
    "google.com",
    "flink.org",
    "starwars.com",
    "gamespot",
    "timeout.com",
    "galactic-voyage.com",
    "screenrant.com",
    "zimbio.com",
    "telegraph.co.uk"
  };

  private String[] metrics = {
    "cpu_usage",
    "memory_usage",
    "jvm_heap_usage",
    "gc_time",
    "gc_collections",
    "disk_read_time",
    "disk_write_time",
    "network_read_time",
    "network_write_time",
    "uptime"
  };

  @Override
  public void run(SourceContext<HostMetric> ctx) throws Exception {

    long timestamp = 0;
    while(isRunning){
      for(int i=0; i<domains.length; i++){
        for(int j=0; j<hostnames.length; j++){
          for(int k=0; k<metrics.length; k++) {
            ctx.collectWithTimestamp(new HostMetric(timestamp, new Hostname(hostnames[j], domains[i]), metrics[k], k), timestamp);
          }
        }
      }
      ctx.emitWatermark(new Watermark(timestamp));
      timestamp++;
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
