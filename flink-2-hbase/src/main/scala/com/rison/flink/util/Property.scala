package com.rison.flink.util

import java.io.{IOException, InputStream, InputStreamReader}
import java.util.Properties

import com.rison.flink.common.Logger

/**
 * @author : Rison 2021/9/22 下午9:33
 *         配置文件工具类
 */
object Property extends Logger {
  private val CONF_NAME = "config.properties"

  private lazy val contextProperties: Properties = new Properties()

  /**
   * 初始化配置
   */
  def init(): Unit = {
    val inputStream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(CONF_NAME)
    try {
      val inputStreamReader = new InputStreamReader(inputStream, "UTF-8")
      contextProperties.load(inputStreamReader)
    } catch {
      case e: IOException => logger.info(">>> Flink 配置文件加载失败，错误信息{}", e.getMessage)
    }
    logger.info(">>>配置文件加载成功！")
  }

  /**
   * 根据key获取value
   *
   * @param key
   * @return
   */
  def getStrValue(key: String): String = {
    init()
    contextProperties.getProperty(key)
  }

  /**
   * 根据key获取value
   *
   * @param key
   * @return
   */
  def getIntValue(key: String): Int = {
    init()
    contextProperties.getProperty(key).toInt
  }

  /**
   * 获取kafka配置信息
   *
   * @param groupId
   * @return
   */
  def getKafkaProperties(groupId: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", getStrValue("kafka.bootstrap.servers"))
    properties.setProperty("zookeeper.connect", getStrValue("kafka.zookeeper.connect"))
    properties.setProperty("group.id", groupId)
    properties
  }

}
