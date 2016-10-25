package com.dataartisans.cookbook.dynamicstreaming.jobs

import com.dataartisans.cookbook.dynamicstreaming.data.{Impression, Program}
import com.dataartisans.cookbook.dynamicstreaming.functions.{DynamicKeyFunction, DynamicCountWindowFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * This program demonstrates how we can do "dynamic" streaming.  In this case
  * data is read from one input source and programs are read from another.  A program
  * publishes how it needs the input stream keyed, as well as a window length, and a
  * window function to run.
  */
object DynamicStreamingJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(dataSource)

    val programStream = env.addSource(programSource).broadcast

    stream
      .connect(programStream)
      .flatMap(new DynamicKeyFunction)
      .keyBy(_.key)
      .connect(programStream)
      .flatMap(new DynamicCountWindowFunction)
      .print()

    env.execute()
  }

  /**
    * Just some test data
    */
  def dataSource = {
    (sc: SourceContext[Impression]) => {
      while (true) {
        Thread.sleep(100)
        sc.collect(Impression(123456789, 24, 10))
        sc.collect(Impression(123456789, 25, 20))
      }
    }
  }

  /**
    * This is an example of a stream of programs.  They do different things
    * and they provide a keySeletor that indicates how they want the input stream
    * keyed.  These are generated at runtime!  In a real system they would be programs
    * streamed in -- maybe groovy or other dynamic JVM language.
    */
  def programSource = {
    (sc: SourceContext[Program[Impression, Long, Long]]) => {

      /**
        * Key by TweetId, window into blocks of 10, and sum
        */
      sc.collect(
        Program(
          keySelector = keySelector(_.tweetId),
          keyName = "tweetId",
          windowLength = 10,
          windowFunction = (itor) => {
            itor.map(_.count).sum  // compute sum -- expect 150
          }
        )
      )

      /**
        * Key by userId, window into blocks of 5, and average
        */
      sc.collect(
        Program(
          keySelector = keySelector(_.userId),
          keyName = "userId",
          windowLength = 5,
          windowFunction = (itor) => {
            itor.map(_.count).sum / itor.size // compute average -- expect 10 & 20
          }
        )
      )

      while (true) {
        Thread.sleep(500)
      }
    }
  }

  private def keySelector[IN, KEY](function: IN => KEY): KeySelector[IN, KEY] = {
    new KeySelector[IN, KEY]() {
      override def getKey(value: IN): KEY = {
        function(value)
      }
    }
  }
}
