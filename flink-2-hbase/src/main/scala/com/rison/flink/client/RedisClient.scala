package com.rison.flink.client

import com.rison.flink.util.Property
import redis.clients.jedis.Jedis

/**
 * @author : Rison 2021/9/25 下午1:43
 *         RedisClient
 */
object RedisClient {
  var jedis: Jedis = _

  def init(): Unit = {
    jedis = new Jedis(Property.getStrValue("redis.host"), Property.getIntValue("redis.port"))
    jedis.select(Property.getIntValue("redis.db"))
  }

  /**
   * 根据key获取数据
   *
   * @param key
   * @return
   */
  def getData(key: String): String = {
    if (jedis == null) {
      init()
    }
    val str: String = jedis.get(key)
    jedis.close()
    str
  }

}
