package com.dataartisans.cookbook.dynamicstreaming.data

import org.apache.flink.api.java.functions.KeySelector

case class Program[IN, KEY, OUT](
                                  keySelector: KeySelector[IN, KEY],
                                  keyName: String,
                                  windowLength: Long,
                                  windowFunction: Iterable[IN] => OUT)