package com.rison.flink.scheduler

import com.rison.flink.client.HbaseClient
import com.rison.flink.common.Constants
import com.rison.flink.domain.ProductPortraitEntity

/**
 * @author : Rison 2021/9/24 下午6:27
 *         基于产品标签的产品的相关度计算
 *         策略2：基于产品标签，计算产品的余弦相似度
 *         w = sqrt( pow((tag{i,a} - tag{j,a}),2)  + pow((tag{i,b} - tag{j,b}),2) )
 */
object ProductCos {


  /**
   * 计算一个产品和其他超的评分，并将计算结果放入到Hbase中
   *
   * @param id
   * @param otherIds
   */
  def getSingleProductCos(id: String, otherIds: List[String]): Unit = {

    val product: ProductPortraitEntity = singleProduct(id)
    for (otherId <- otherIds) {
      if (!id.equals(otherId)) {
        val otherProduct: ProductPortraitEntity = singleProduct(otherId)
        val score: Double = getScore(product, otherProduct)
        HbaseClient.putData("ps", id, "p", otherId, score.toString)
      }
    }

  }

  /**
   * 计算一个产品的所有的标签数据
   *
   * @param productId 产品id
   * @return
   */
  def singleProduct(productId: String): ProductPortraitEntity = {
    val entity = new ProductPortraitEntity()
    val woman: String = HbaseClient.getData("prod", productId, "sex", Constants.SEX_WOMAN)
    val man: String = HbaseClient.getData("prod", productId, "sex", Constants.SEX_WOMAN)
    val age_10: String = HbaseClient.getData("prod", productId, "age", Constants.AGE_10)
    val age_20: String = HbaseClient.getData("prod", productId, "age", Constants.AGE_20)
    val age_30: String = HbaseClient.getData("prod", productId, "age", Constants.AGE_30)
    val age_40: String = HbaseClient.getData("prod", productId, "age", Constants.AGE_40)
    val age_50: String = HbaseClient.getData("prod", productId, "age", Constants.AGE_50)
    val age_60: String = HbaseClient.getData("prod", productId, "age", Constants.AGE_60)
    entity.man = man.toInt
    entity.woman = woman.toInt
    entity.age_10 = age_10.toInt
    entity.age_20 = age_20.toInt
    entity.age_30 = age_30.toInt
    entity.age_40 = age_40.toInt
    entity.age_50 = age_50.toInt
    entity.age_60 = age_60.toInt
    entity
  }

  /**
   * 根据标签计算两个产品的相关度
   *
   * @param product      产品
   * @param otherProduct 相关产品
   */
  def getScore(product: ProductPortraitEntity, otherProduct: ProductPortraitEntity) = {
    val sqrt: Double = Math.sqrt(product.getTotal + otherProduct.getTotal)
    if (sqrt == 0) 0.0
    val total: Int = product.man * otherProduct.man +
      product.woman * otherProduct.woman +
      product.age_10 * otherProduct.age_10 +
      product.age_20 * otherProduct.age_20 +
      product.age_30 * otherProduct.age_30 +
      product.age_40 * otherProduct.age_40 +
      product.age_50 * otherProduct.age_50 +
      product.age_60 * otherProduct.age_60
    Math.sqrt(total) / sqrt;
  }
}

