package com.rison.flink.scheduler

import com.rison.flink.client.HbaseClient

/**
 * @author : Rison 2021/9/24 下午4:18
 *         基于协同过滤的产品相关度计算
 *         策略1 ：协同过滤
 *         abs( i ∩ j)
 *         w = ——————————————
 *         sqrt(i || j)
 */
object ItemCF {
  /**
   * 计算一个产品和其他相关产品的评分，将结果放入到hbase
   *
   * @param id       产品id
   * @param otherIds 其他产品id
   */
  def getSingleItemCF(id: String, otherIds: List[String]): Unit = {
    for (otherId <- otherIds) {
      if (!id.equals(otherId)) {
        val score = twoItemCF(id, otherId)
        HbaseClient.putData("px", id, "p", otherId, score.toString)
      }
    }
  }

  /**
   * 两个产品之间的评分
   *
   * @param id
   * @param otherId
   * @return
   */
  def twoItemCF(id: String, otherId: String): Double = {
    //(String, Double) => 其实就是(userId, 浏览次数times)
    val idContext: List[(String, Double)] = HbaseClient.getRow("p_history", id)
    val otherIdContext: List[(String, Double)] = HbaseClient.getRow("p_history", otherId)
    var sum = 0;
    val n = idContext.size
    val m = otherIdContext.size
    val total: Double = Math.sqrt(n * m)
    for (x <- idContext) {
      val userId = x._1
      for (y <- otherIdContext) {
        if (userId.equals(y._1)) {
          sum ++
        }
      }
    }
    if (total == 0) {
      0.0
    }
    sum / total
  }
}
