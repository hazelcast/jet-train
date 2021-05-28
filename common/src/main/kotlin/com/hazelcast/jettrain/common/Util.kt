@file:JvmName("UtilKt")

package com.hazelcast.jettrain.common

import com.hazelcast.function.FunctionEx
import com.hazelcast.function.PredicateEx
import kotlin.random.Random

fun sampleEvery(frequency: Int) = PredicateEx<Any> { Random.nextInt(frequency) == 0 }
val toStringFn = FunctionEx<Any?, String> { it?.toString() }