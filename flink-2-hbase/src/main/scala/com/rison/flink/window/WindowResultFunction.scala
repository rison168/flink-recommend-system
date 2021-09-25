package com.rison.flink.window

import java.lang

import com.rison.flink.domain.TopProductEntity
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/6/15 上午9:05
 *
 */
case class WindowResultFunction() extends ProcessWindowFunction[Long, TopProductEntity, Int, TimeWindow] {
  override def process(key: Int, context: Context, elements: Iterable[Long], out: Collector[TopProductEntity]): Unit = {
    val times: Long = elements.iterator.next()
    val end: Long = context.window.getEnd
    out.collect(TopProductEntity.of(key, `end`, times))
  }
}