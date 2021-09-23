package com.rison.flink.map

import java.sql.ResultSet

import com.rison.flink.client.{HbaseClient, MySqlClient}
import com.rison.flink.common.{AgeType, LogToEntity}
import com.rison.flink.domain.LogEntity
import org.apache.flink.api.common.functions.MapFunction

/**
 * @author : Rison 2021/9/23 上午8:38
 *
 */
case class ProductPortraitMapFunction() extends MapFunction[String, String] {
  override def map(logStr: String): String = {
    val logEntity: LogEntity = LogToEntity.getLogEntity(logStr)
    val resultSet: ResultSet = MySqlClient.selectUserById(logEntity.userId)
    if (resultSet != null) {
      while (resultSet.next) {
        val productId: String = logEntity.productId.toString
        val sex: String = resultSet.getString("sex")
        val age: String = resultSet.getString("age")
        //TODO: 写入到Hbase,sex列簇的字段（woman,man）/age列簇的字段(0s,10s,20s,30s,40s,50s,60s)次数单调递增
        HbaseClient.increaseColumn(tablename = "prod", rowkey = productId, familyname = "sex", sex)
        HbaseClient.increaseColumn(tablename = "prod", rowkey = productId, familyname = "age", AgeType.getAageType(age.toInt))
      }
    }
    null
  }
}
