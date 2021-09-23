package com.rison.flink.task

import java.util.Properties

import com.rison.flink.map.ProductPortraitMapFunction
import com.rison.flink.util.Property
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author : Rison 2021/9/23 上午8:29
 *         产品画像写入到Hbase
 *         用两个维度记录产品画像，一个是喜爱该产品的年龄段、一个是性别
 *         数据存储在Hbase的prod表
 */
object ProductPortraitTask {
  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  private val properties: Properties = Property.getKafkaProperties("productPortrait")
  private val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
  dataStream.map(ProductPortraitMapFunction())
  env.execute("Product Portrait Task")

}
