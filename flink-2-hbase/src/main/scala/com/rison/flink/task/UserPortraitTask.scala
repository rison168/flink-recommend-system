package com.rison.flink.task

import java.util.Properties

import com.rison.flink.util.Property
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author : Rison 2021/9/23 下午9:29
 *         用户画像数据写入到HBase
 */
object UserPortraitTask {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = Property.getKafkaProperties("userPortrait")
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
    dataStream.map(UserPortraitMapFunction())

  }

}
