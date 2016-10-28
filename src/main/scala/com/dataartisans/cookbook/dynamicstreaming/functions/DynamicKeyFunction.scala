package com.dataartisans.cookbook.dynamicstreaming.functions

import com.dataartisans.cookbook.dynamicstreaming.data.{CompositeKey, Keyed, Program}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class DynamicKeyFunction[IN, KEY, OUT] extends RichCoFlatMapFunction[IN, Program[IN, KEY, OUT], Keyed[IN, KEY]] {

  private val programs = mutable.Set[Program[IN, KEY, OUT]]()

  override def flatMap1(data: IN, out: Collector[Keyed[IN, KEY]]): Unit = {
    // for each program output a message with keyed according to the needs of the program
    programs
      .foreach(p => {
        out.collect(Keyed(data, CompositeKey(p.keyName, p.keySelector.getKey(data))))
      })
  }

  override def flatMap2(program: Program[IN, KEY, OUT], out: Collector[Keyed[IN, KEY]]): Unit = {
    programs.add(program)
  }
}
