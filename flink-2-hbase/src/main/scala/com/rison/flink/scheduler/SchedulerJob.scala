package com.rison.flink.scheduler

import java.util.concurrent.{ExecutorService, ScheduledExecutorService, ScheduledThreadPoolExecutor, TimeUnit}

import com.rison.flink.client.HbaseClient
import com.rison.flink.common.Logger

/**
 * @author : Rison 2021/9/24 下午2:30
 *         任务定时器实现推荐策略计算产品评分计算
 *         每12小时定时调度一次 基于两个推荐策略的 产品评分计算
 *         策略1 ：协同过滤
 *         数据写入Hbase表  px
 *         策略2 ： 基于产品标签 计算产品的余弦相似度
 *         数据写入Hbase表 ps
 *
 * */
*/

object SchedulerJob extends Logger {
  val scheduledExecutorService: ScheduledExecutorService = new ScheduledThreadPoolExecutor(1)

  def task(): Unit = {
    logger.info("===============开始执行定时推荐策略任务=================")
    //TODO 取出被用户点击过的产品id,getAllKey,其实真实情况不太可能把所有产品取出来
    val productIdList: List[String] = HbaseClient.getAllKey("p_history")
    for (id <- productIdList) {
      ItemCF.getSingleItemCF(id, productIdList)
      ProductCos.getSingleProductCos(id, productIdList)
    }
    logger.info("===============定时任务结束=========================")
  }

  def job(): Unit = {
    scheduledExecutorService.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        task()
      }
    }, 0, 12, TimeUnit.HOURS)
  }

  def main(args: Array[String]): Unit = {
    job()
  }
}
