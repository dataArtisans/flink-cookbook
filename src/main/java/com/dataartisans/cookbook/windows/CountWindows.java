package com.dataartisans.cookbook.windows;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CountWindows {
  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Tuple2<String, Integer>> sourceStream = env.fromElements(
      new Tuple2("a", 1),
      new Tuple2("a", 1),
      new Tuple2("a", 1),
      new Tuple2("a", 1),
      new Tuple2("a", 1),
      new Tuple2("b", 2),
      new Tuple2("b", 2),
      new Tuple2("b", 2),
      new Tuple2("b", 2),
      new Tuple2("b", 2));

    sourceStream
      .keyBy(0)
      .countWindow(5)
      .sum(1)
      .print();

    // execute program
    env.execute("Count Windows");
  }
}
