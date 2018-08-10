package com.dataartisans.cookbook.misc

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class Event(guid: String, state: String)

object ProcessingTimeTriggerWithPeriodicFirings {
  def apply(intervalMs: Long) = {
    new ProcessingTimeTriggerWithPeriodicFirings(intervalMs)
  }
}

class ProcessingTimeTriggerWithPeriodicFirings(intervalMs: Long) extends Trigger[Event, TimeWindow] {
  private val startTimeDesc = new ValueStateDescriptor[Long]("start-time", classOf[Long], 0L)

  override def onElement(element: Event, timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    val startTime = ctx.getPartitionedState(startTimeDesc)
    if (startTime.value == 0) {
      startTime.update(window.getStart)
      ctx.registerProcessingTimeTimer(window.getEnd)
      ctx.registerProcessingTimeTimer(System.currentTimeMillis() + intervalMs)
    }
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    if (time == window.getEnd) {
      TriggerResult.PURGE
    }
    else {
      ctx.registerProcessingTimeTimer(time + intervalMs)
      TriggerResult.FIRE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(w: TimeWindow, triggerContext: TriggerContext): Unit = ???
}

object StateTracking {
  def main(args: Array[String]) {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // TODO: Remove
    env.setParallelism(1)

    val stream = env.addSource((ctx: SourceContext[Event]) => {
      var guid = 1
      while (true) {
        for (i <- 1 to 1) {
          val state = s"state-$i"
          ctx.collect(Event(s"guid-$guid", state))
          Thread.sleep(1000)
        }
        guid = guid + 1
      }
    })

    val results = stream
      .keyBy(_.guid)
      .timeWindow(Time.seconds(30))
      .trigger(ProcessingTimeTriggerWithPeriodicFirings(1000))
      .apply(
        (e1, e2) => e2,
        (k, w, i, c: Collector[(String, Long)]) => {
          if (i.head != null) c.collect((i.head.state, 1))
        }
      )
      .keyBy(0)
      .timeWindow(Time.seconds(1))
      .sum(1)


    results.printToErr()

    env.execute("Job")
  }

}
