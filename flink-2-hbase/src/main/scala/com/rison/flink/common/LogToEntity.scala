package com.rison.flink.common

import com.rison.flink.domain.LogEntity

/**
 * @author : Rison 2021/9/22 下午10:23
 *         日志字符串转日志实体类
 */
object LogToEntity {

  def getLogEntity(logStr: String): LogEntity = {
    val logArr: Array[String] = logStr.split(",")
    if (logArr.length < 2) {
      null
    }
    LogEntity(logArr(0).toInt, logArr(1).toInt, logArr(2).toLong, logArr(3).toString)
  }

}
