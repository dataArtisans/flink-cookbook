package com.dataartisans.cookbook.dynamicstreaming.functions

import com.dataartisans.cookbook.dynamicstreaming.data.{CompositeKey, Program, Keyed}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class DynamicCountWindowFunction[IN, KEY, OUT] extends RichCoFlatMapFunction[Keyed[IN, KEY], Program[IN, KEY, OUT], (CompositeKey[KEY], OUT)] {

  private case class KeyState(var count: Long, val items: mutable.Buffer[IN])

  // The program to run for each different keyed stream (extend to many)
  private val programs = mutable.Map[String, Program[IN, KEY, OUT]]()
  private val keyState = mutable.Map[CompositeKey[KEY], KeyState]()

  override def flatMap1(data: Keyed[IN, KEY], out: Collector[(CompositeKey[KEY], OUT)]): Unit = {

    // Get the program to run
    val program = programs(data.key.name)

    // Retrieve the state for the current key
    val state = stateForKey(data.key)

    // Fire the window if the program-specified count has been reached
    if (state.count < program.windowLength) {
      state.items += data.wrapped
      state.count += 1
    }
    else {
      keyState.remove(data.key)
      // Fire the window function
      out.collect(data.key, program.windowFunction(state.items))
    }
  }

  override def flatMap2(program: Program[IN, KEY, OUT], out: Collector[(CompositeKey[KEY], OUT)]): Unit = {
    programs.put(program.keyName, program)
  }

  private def stateForKey(key: CompositeKey[KEY]): KeyState = {
    keyState.getOrElseUpdate(key, KeyState(0, emptyBuffer))
  }

  private def emptyBuffer = mutable.Buffer.empty[IN]
}

