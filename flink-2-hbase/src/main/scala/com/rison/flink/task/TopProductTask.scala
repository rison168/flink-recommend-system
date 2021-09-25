package com.rison.flink.task

import java.util.Properties

import com.rison.flink.aggregate.CountAggregate
import com.rison.flink.domain.{LogEntity, TopProductEntity}
import com.rison.flink.map.TopProductMapFunction
import com.rison.flink.sink.TopNRedisSink
import com.rison.flink.top.TopNHotItem
import com.rison.flink.util.Property
import com.rison.flink.window.WindowResultFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/9/25 下午1:36
 *         热门商品抽取到Redis
 *
 */
object TopProductTask {
  val topSize: Int = 5

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //开启EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost(Property.getStrValue("redis.host"))
      .setPort(Property.getIntValue("redis.port"))
      .setDatabase(Property.getIntValue("redis.db"))
      .build()
    val properties: Properties = Property.getKafkaProperties("topProduct")
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
    dataStream.map(TopProductMapFunction())
      //抽取时间戳作为watermark, 以秒为单位
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[LogEntity]() {
        override def extractAscendingTimestamp(log: LogEntity) = log.time * 1000
      })
      //按照productId 按滑动窗口
      .keyBy(_.productId)
      .timeWindow(Time.seconds(60), Time.seconds(5))
      .aggregate(CountAggregate(), WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(TopNHotItem(topSize))
      .flatMap {
        (list, out: Collector[TopProductEntity]) =>
          list.foreach(
            pid => {
              out.collect(TopProductEntity(pid.toInt, pid))
            }
          )
      }
      .addSink(new RedisSink[TopProductEntity](config, TopNRedisSink()))

    env.execute("hot topN task")

  }
}
