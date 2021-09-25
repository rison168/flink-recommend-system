package com.rison.flink.map

import com.rison.flink.common.LogToEntity
import com.rison.flink.domain.LogEntity
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/9/25 下午2:01
 *
 */
case class TopProductMapFunction() extends MapFunction[String, LogEntity] {
  override def map(log: String): LogEntity = {
    val logEntity: LogEntity = LogToEntity.getLogEntity(log)
    logEntity
  }
}
