package com.rison.flink.sink

import com.rison.flink.domain.TopProductEntity
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author : Rison 2021/9/25 下午4:29
 *
 */
case class TopNRedisSink() extends RedisMapper[TopProductEntity]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  override def getKeyFromData(t: TopProductEntity): String = {
    t.rankName
  }

  override def getValueFromData(t: TopProductEntity): String = {
    t.productId.toString
  }
}
