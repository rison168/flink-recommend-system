package com.rison.flink.task

import java.util.Properties

import com.rison.flink.map.LogMapFunction
import com.rison.flink.util.Property
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


/**
 * @author : Rison 2021/9/22 下午9:28
 *         将日志写到Hbase
 */
object LogTask {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = Property.getKafkaProperties("log")
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
    dataStream.map(LogMapFunction())
    env.execute("log message receive")
  }

}
