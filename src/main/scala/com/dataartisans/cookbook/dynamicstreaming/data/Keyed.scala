package com.dataartisans.cookbook.dynamicstreaming.data

case class Keyed[IN, KEY](wrapped: IN, key: CompositeKey[KEY])
