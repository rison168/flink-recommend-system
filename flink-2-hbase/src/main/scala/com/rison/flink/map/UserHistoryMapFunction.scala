package com.rison.flink.map

import com.rison.flink.client.HbaseClient
import com.rison.flink.common.LogToEntity
import com.rison.flink.domain.LogEntity
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/9/23 下午10:08
 *         将 用户-产品、、产品-用户分别存储到Hbase表
 */
case class UserHistoryMapFunction() extends MapFunction[String, String] {
  override def map(log: String): String = {
    val logEntity: LogEntity = LogToEntity.getLogEntity(log)
    if (logEntity != null) {
      HbaseClient.increaseColumn(tablename = "u_history", logEntity.userId.toString, "p", logEntity.productId.toString)
      HbaseClient.increaseColumn(tablename = "p_history", logEntity.productId.toString, "p", logEntity.userId.toString)
    }
    null
  }
}
