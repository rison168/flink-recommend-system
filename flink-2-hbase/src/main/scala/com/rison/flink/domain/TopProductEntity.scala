package com.rison.flink.domain

/**
 * @author : Rison 2021/9/22 下午5:06
 *         热门产品
 */
class TopProductEntity() extends Serializable {
  var productId: Int = _
  var actionTimes: Int = _
  var windowEnd: Long = _
  var rankName: String = _

  def this(productId: Int, actionTimes: Int, windowEnd: Long, rankName: String) = {
    this()
    this.productId = productId
    this.actionTimes = actionTimes
    this.windowEnd = windowEnd
    this.rankName = rankName
  }
}

object TopProductEntity {

  def apply(): TopProductEntity = new TopProductEntity()

  def of(itemId: Int, end: Long, count: Long): TopProductEntity = {
    val entity: TopProductEntity = TopProductEntity()
    entity.actionTimes = count.toInt
    entity.productId = itemId
    entity.windowEnd = `end`
    entity.rankName = end.toString
    entity
  }


}
