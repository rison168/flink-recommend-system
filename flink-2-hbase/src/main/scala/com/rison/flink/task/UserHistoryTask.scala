package com.rison.flink.task

import java.util.Properties

import com.rison.flink.map.UserHistoryMapFunction
import com.rison.flink.util.Property
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author : Rison 2021/9/23 下午10:03
 *         用户浏览历史数据，更新到Hbase
 *         通过Flink去记录用户浏览过这个类目下的那些产品，
 *         为后面的基于协同过滤做准备，实时记录用户的评分到Hbase中，
 *         为后续离线处理做准备。
 *         数据存储在Hbase的p_history表
 */
object UserHistoryTask {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = Property.getKafkaProperties("history")
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
    dataStream.map(new UserHistoryMapFunction())
    env.execute("user product history task")
  }
}
