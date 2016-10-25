package com.dataartisans.cookbook.windows;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ProcessingTimeWindows {
  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    env.setParallelism(1);

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
      .map( t -> {Thread.sleep(500); return t; })
      .returns(new TypeHint<Tuple2<String, Integer>>(){})
      .keyBy(0)
      .timeWindow(Time.seconds(1))
      .sum(1)
      .addSink(new PrintSinkFunction<>(true));

    // execute program
    env.execute("Processing Time Windows");
  }
}
