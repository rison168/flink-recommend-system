package com.rison.flink.task

import java.util.Properties

import com.rison.flink.map.{GetLogMapFunction, UserHistoryWithInterestMapFunction}
import com.rison.flink.util.Property
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author : Rison 2021/9/24 上午8:59
 *         用户兴趣
 *         根据用户对同一个商品操作计算兴趣度，
 *         计算规则通过操作时间的间隔（如：购物 - 浏览 < 100s）则判定为一定兴趣事件，
 *         通过Flink的valueState实现，如果用户的操作action=3(购物)，则清除这个产品的state,
 *         如果超过100s没有出现action=3的事件，也会清除这个state。
 *         数据存储在Hbase的u_interest表
 */
object UserInterestTask {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = Property.getKafkaProperties("interest")
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
    dataStream.map(GetLogMapFunction())
      .keyBy("userId")
      .map(UserHistoryWithInterestMapFunction())
    env.execute("user product interest task")
  }
}
