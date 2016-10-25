package com.dataartisans.cookbook.multiaggregate;

public class HostMetric {
  private long timestamp = 0;
  private Hostname hostname = new Hostname();
  private String metricName = "";
  private double value = 0.0;

  public HostMetric() {

  }

  public HostMetric(long timestamp, Hostname hostname, String metricName, double value) {
    this.timestamp = timestamp;
    this.hostname = hostname;
    this.metricName = metricName;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Hostname getHostname() {
    return hostname;
  }

  public void setHostname(Hostname hostname) {
    this.hostname = hostname;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "HostMetric{" +
      "timestamp=" + timestamp +
      ", hostname=" + hostname +
      ", metricName='" + metricName + '\'' +
      ", value=" + value +
      '}';
  }

}
