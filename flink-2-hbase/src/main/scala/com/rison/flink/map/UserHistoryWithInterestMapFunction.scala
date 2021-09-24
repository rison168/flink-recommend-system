package com.rison.flink.map

import com.rison.flink.client.HbaseClient
import com.rison.flink.domain.LogEntity
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration

/**
 * @author : Rison 2021/9/24 上午9:08
 *
 */
case class UserHistoryWithInterestMapFunction() extends RichMapFunction[LogEntity, String] {
  var state: ValueState[Action] = _

  override def open(parameters: Configuration): Unit = {
    //TODO 设置state的过期时间为100s
    val ttlConfig: StateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(100L))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()
    val desc = new ValueStateDescriptor[Action]("actionTime", classOf[Action])
    desc.enableTimeToLive(ttlConfig)
    state = this.getRuntimeContext.getState(desc)
  }

  override def map(log: LogEntity): String = {
    var actionLastTime: Action = state.value()
    val actionCurrentTime = Action(log.action, log.time.toString)
    var times = 1;
    //如果用户没有操作，则为state创建值
    if (actionLastTime == null) {
      state.update(actionCurrentTime)
      actionLastTime = actionCurrentTime
      //浏览产品一次,权重加一
      saveToHBase(log, times)
    } else {
      times = getTimesByRule(actionLastTime, actionCurrentTime)
      saveToHBase(log, times)
    }
    //如果用户的操作为3(购物)，则清除这个key state
    if (actionCurrentTime.`type`.equals("3")) {
      state.clear()
    }
    null
  }

  def saveToHBase(log: LogEntity, times: Int): Unit = {
    if (log != null) {
      for (i <- 0 until times) {
        HbaseClient.increaseColumn("u_interest", log.userId.toString, "p", log.productId.toString)
      }
    }
  }

  /**
   * 感兴趣产品计算规则，
   * 如果动作连续发生且时间很短(小于100秒内完成动作),
   * 则标注为用户对此产品兴趣度很高（给相应的times权重）
   *
   * @param actionLastTime
   * @param actionCurrentTime
   * @return
   */
  def getTimesByRule(actionLastTime: Action, actionCurrentTime: Action): Int = {
    //TODO 动作主要有3种类型 1-> 浏览，2->分享，3->购物
    val lastTimeType: Int = actionLastTime.`type`.toInt
    val currentTimeType: Int = actionCurrentTime.`type`.toInt
    val lastTime: Long = actionLastTime.time.toLong
    val currentTime: Long = actionCurrentTime.time.toLong

    var times = 1

    // 如果动作连续发生且时间很短(小于100秒内完成动作), 则标注为用户对此产品兴趣度很高
    if (currentTimeType > lastTimeType && (currentTimeType - lastTime < 100000L)) {
      times *= currentTimeType - lastTimeType
    }
    times
  }
}


/**
 * 动作类 记录动作类型和动作发生时间（Event Time）
 *
 * @param `type`
 * @param time
 */
case class Action(`type`: String, time: String)
