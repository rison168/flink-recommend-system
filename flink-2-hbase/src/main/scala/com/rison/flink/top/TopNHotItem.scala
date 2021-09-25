package com.rison.flink.top

import com.rison.flink.domain.TopProductEntity
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author : Rison 2021/9/25 下午3:33
 *
 */
case class TopNHotItem(topNum: Int) extends KeyedProcessFunction[Long, TopProductEntity, List[String]] {
  var itemsState: ListState[TopProductEntity] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //状态注册
    val itemsState: ListState[TopProductEntity] = getRuntimeContext
      .getListState[TopProductEntity](new ListStateDescriptor[TopProductEntity]("itemState_state", classOf[TopProductEntity]))
  }

  override def processElement(i: TopProductEntity, context: KeyedProcessFunction[Long, TopProductEntity, List[String]]#Context, collector: Collector[List[String]]): Unit = {
    itemsState.add(i)
    //注册windowEnd + 1 的 eventTimer ,当触发时，说明收齐了属于WindowEnd的产品数据
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, TopProductEntity, List[String]]#OnTimerContext, out: Collector[List[String]]): Unit = {
    val allItems: ListBuffer[TopProductEntity] = ListBuffer[TopProductEntity]()
    itemsState.get().forEach(
      item => {
        allItems.append(item)
      }
    )
    //提前清除状态中的数据，释放空间
    itemsState.clear()
    //排序，点击量从大到小排序
    val sortItemList: List[String] = allItems.sortWith(_.actionTimes > _.actionTimes).toList.toStream.map(item => item.productId.toString).toList
    out.collect(sortItemList)
  }
}
