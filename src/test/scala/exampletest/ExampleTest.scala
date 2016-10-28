package exampletest

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.api.scala._

import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

/**
  * Tests for Folds over windows. These also test whether OutputTypeConfigurable functions
  * work for windows, because FoldWindowFunction is OutputTypeConfigurable.
  */
class ExampleTest extends StreamingMultipleProgramsTestBase {

  @Test
  def testReduceWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
        ctx.collect(("a", 0))
        ctx.collect(("a", 1))
        ctx.collect(("a", 2))
        ctx.collect(("b", 3))
        ctx.collect(("b", 4))
        ctx.collect(("b", 5))
        ctx.collect(("a", 6))
        ctx.collect(("a", 7))
        ctx.collect(("a", 8))

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }

      override def cancel(): Unit = {}
    }).assignTimestampsAndWatermarks(new ExampleTest.Tuple2TimestampExtractor)

    source1
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      .addSink(new SinkFunction[(String, Int)]() {
        def invoke(value: (String, Int)) {
          ExampleTest.testResults += value.toString
        }
      })

    env.execute("Reduce Window Test")

    val expectedResult = mutable.MutableList(
      "(aaa,3)",
      "(aaa,21)",
      "(bbb,12)")

    assertEquals(expectedResult.sorted, ExampleTest.testResults.sorted)
  }
}

object ExampleTest {

  val testResults = mutable.MutableList[String]()

  private class Tuple2TimestampExtractor extends AssignerWithPunctuatedWatermarks[(String, Int)] {

    private var currentTimestamp = -1L

    override def extractTimestamp(element: (String, Int), previousTimestamp: Long): Long = {
      currentTimestamp = element._2
      currentTimestamp
    }

    def checkAndGetNextWatermark(lastElement: (String, Int), extractedTimestamp: Long): Watermark = {
      new Watermark(lastElement._2 - 1)
    }
  }
}


