package com.rison.flink.map

import com.rison.flink.common.LogToEntity
import com.rison.flink.domain.LogEntity
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/9/24 上午9:05
 *         将log字符串转换为对象
 */
case class GetLogMapFunction() extends MapFunction[String, LogEntity] {
  override def map(log: String): LogEntity = {
    val logEntity: LogEntity = LogToEntity.getLogEntity(log)
    logEntity
  }
}
