package com.rison.flink.map

import java.sql.ResultSet

import com.rison.flink.client.{HbaseClient, MySqlClient}
import com.rison.flink.common.LogToEntity
import com.rison.flink.domain.LogEntity
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/9/23 下午9:34
 *         用户画像，写入到Hbase
 */
case class UserPortraitMapFunction() extends MapFunction[String, String] {
  override def map(log: String): String = {
    val logEntity: LogEntity = LogToEntity.getLogEntity(log)
    val resultSet: ResultSet = MySqlClient.selectByProductionId(logEntity.productId)
    if (resultSet != null) {
      while (resultSet.next()) {
        val userId: String = logEntity.userId.toString
        //产地
        val country: String = resultSet.getString("country")
        HbaseClient.increaseColumn(tablename = "user", rowkey = userId, familyname = "country", country)
        //颜色
        val color: String = resultSet.getString("color")
        HbaseClient.increaseColumn(tablename = "user", rowkey = userId, familyname = "color", color)
        //风格
        val style: String = resultSet.getString("style")
        HbaseClient.increaseColumn(tablename = "user", rowkey = userId, familyname = "style", style)
      }
    }
    null
  }
}
