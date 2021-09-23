package com.rison.flink.common

/**
 * @author : Rison 2021/9/23 上午10:55
 *         age 类型转换
 */
object AgeType {

  def getAageType(age: Int): String = {
    if (10 <= age && age < 20) "10s"
    else if (20 <= age && age < 30) "20s"
    else if (30 <= age && age < 40) "20s"
    else if (40 <= age && age < 50) "20s"
    else if (50 <= age && age < 60) "20s"
    else "0s"
  }

}
