package com.rison.flink.domain

/**
 * @author : Rison 2021/9/22 下午5:02
 *         产品画像
 */

class ProductPortraitEntity() extends Serializable {
  var man = 0
  var woman = 0

  var age_10 = 0
  var age_20 = 0
  var age_30 = 0
  var age_40 = 0
  var age_50 = 0
  var age_60 = 0

  def getTotal: Int = {
    var ret = 0
    ret += (man * man) + (woman * woman) + (age_10 * age_10) + (age_20 * age_20) + (age_30 * age_30) + (age_40 * age_40) + (age_50 * age_50) + (age_60 * age_60)
    ret
  }
}

