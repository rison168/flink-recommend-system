package com.rison.flink.map

import com.rison.flink.common.{LogToEntity, Logger}
import com.rison.flink.domain.LogEntity
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/9/22 下午10:09
 *
 */
case class LogMapFunction() extends MapFunction[String, LogEntity] with Logger{
  override def map(logStr: String): LogMapFunction = {
    logger.info("日志信息：{}", logStr)
    val logEntity: LogEntity = LogToEntity.getLogEntity(logStr)
    //TODO: 写入到Hbase里面去
    if (logEntity != null){

    }


  }
}