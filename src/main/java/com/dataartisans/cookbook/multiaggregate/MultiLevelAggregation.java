package com.dataartisans.cookbook.multiaggregate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class MultiLevelAggregation {

  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    //env.setParallelism(1);

    DataStreamSource<HostMetric> sourceStream = env.addSource(new HostMetricGenerator());

    DataStream<HostMetric> hostStream = sourceStream
      //.filter(hm -> hm.getMetricName().equals("memory_usage"))
      .keyBy("hostname", "metricName")
      .timeWindow(Time.seconds(1))
      .sum("value");

    DataStream<HostMetric> domainStream = hostStream
      .keyBy("hostname.domain", "metricName")
      .timeWindow(Time.seconds(1))
      .sum("value")
      .map(elideHost());

    DataStream<HostMetric> metricStream = domainStream
      .keyBy("metricName")
      .timeWindow(Time.seconds(1))
      .sum("value")
      .map(elideHostAndDomain());

    boolean mergedOutput = false;
    if (mergedOutput) {
      union(hostStream, domainStream, metricStream).addSink(new PrintSinkFunction<>(false));
    } else {
      hostStream.addSink(new PrintSinkFunction(false));
      domainStream.addSink(new PrintSinkFunction<>(false));
      metricStream.addSink(new PrintSinkFunction<>(true));
    }

    // execute program
    env.execute("Name");
  }

  private static DataStream<HostMetric> union(DataStream<HostMetric> hostStream, DataStream<HostMetric> domainStream, DataStream<HostMetric> metricStream) {
    return hostStream
      .union(domainStream)
      .union(metricStream);
  }

  private static MapFunction<HostMetric, HostMetric> elideHost() {
    return hostMetric -> {
      hostMetric.getHostname().setHost("*");
      return hostMetric;
    };
  }

  private static MapFunction<HostMetric, HostMetric> elideHostAndDomain() {
    return hostMetric -> {
      hostMetric.getHostname().setHost("*");
      hostMetric.getHostname().setDomain("*");
      return hostMetric;
    };
  }
}
