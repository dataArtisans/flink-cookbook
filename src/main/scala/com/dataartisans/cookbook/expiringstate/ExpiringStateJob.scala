package com.dataartisans.cookbook.expiringstate

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

object ExpiringStateJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource((ctx: SourceContext[Data]) => {
      val MessageIntervalMs = 100
      var key = 0
      var timestamp = 0L
      while (true) {
        for (i <- 0 until 10) {
          ctx.collectWithTimestamp(Data(timestamp, key, 1), timestamp)
          ctx.emitWatermark(new Watermark(timestamp))
          Thread.sleep(MessageIntervalMs)
          timestamp += MessageIntervalMs
        }
        // keyspace is always growing but we only see each key for 1 second and then never again
        key += 1
      }
    })

    // Doing this checkpoints will grow larger and larger as the keyspace grows forever and will
    // never get cleaned up.
//        stream
//          .keyBy("key")
//          .reduce((a, b) => Data(a.timestamp, a.key, a.value + b.value))
//          .print()

    // Here's the same thing with a custom trigger that cleans up state for each key after 2 seconds
    stream
      .keyBy("key")
      .window(GlobalWindows.create())
      .trigger(new StateTtlTrigger(2000))
      .reduce((a, b) => Data(a.timestamp, a.key, a.value + b.value))
      .print

    env.execute()
  }

  case class Data(timestamp: Long, key: Long, value: Long)

  /**
    * This trigger fires the window on every element and registers a timer when it's first created
    * that will purge/destory the window after `expireTimeMs`.  Since the state kept is scoped by window
    * and key the state will be cleaned up when the window is purged.
    *
    * This essentially has the effect of creating state with a TTL.
    *
    * @param expireTimeMs
    */
  class StateTtlTrigger(expireTimeMs: Long) extends Trigger[Data, GlobalWindow] {

    val stateDesc = new ValueStateDescriptor[Boolean]("timer_created", TypeInformation.of(new TypeHint[Boolean]() {}), false)

    override def onElement(t: Data, l: Long, w: GlobalWindow, triggerContext: TriggerContext): TriggerResult = {
      val timerCreated = triggerContext.getPartitionedState(stateDesc)
      if (!timerCreated.value()) {
        println(s"Registering purge timer for key ${t.key}")
        triggerContext.registerEventTimeTimer(t.timestamp + expireTimeMs)
        timerCreated.update(true)
      }
      TriggerResult.FIRE
    }

    override def onEventTime(l: Long, w: GlobalWindow, triggerContext: TriggerContext): TriggerResult = {
      println(s"Purging state for key ${(l - expireTimeMs) / 1000} @ time ${l}")
      triggerContext.getPartitionedState(stateDesc).clear()
      triggerContext.deleteEventTimeTimer(l)
      TriggerResult.PURGE
    }

    override def onProcessingTime(l: Long, w: GlobalWindow, triggerContext: TriggerContext): TriggerResult = {
      ???
    }

    override def clear(w: GlobalWindow, triggerContext: TriggerContext): Unit = ???
  }
}
