package com.rison.flink.client

import java.sql.{Connection, Driver, DriverManager, ResultSet, SQLException, Statement}
import com.rison.flink.common.Logger
import com.rison.flink.util.Property

/**
 * @author : Rison 2021/9/23 上午8:50
 *         Mysql 客户端
 */
object MySqlClient extends Logger {

  private val URL: String = Property.getStrValue("mysql.url")
  private val NAME: String = Property.getStrValue("mysql.name")
  private val PASSWORD: String = Property.getStrValue("mysql.pass")
  private var stmt: Statement = _
  private var connection: Connection = _

  /**
   * 初始化
   */
  def init() = {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      connection = DriverManager.getConnection(URL, NAME, PASSWORD)
      stmt = connection.createStatement()
    } catch {
      case e: ClassNotFoundException => logger.warn("mysql初始化失败！错误日志：{}", e.getMessage)
      case e: SQLException => logger.warn("mysql初始化失败！错误日志：{}", e.getMessage)
    }
  }

  /**
   * 根据用户ID查询用户信息
   *
   * @param userId
   * @return
   */
  def selectUserById(userId: Int): ResultSet = {
    if (stmt == null) {
      init()
    }
    val sql: String = String.format("select * from user where user_id = %s", userId.toString)
    val resultSet: ResultSet = stmt.executeQuery(sql)
    connection.close()
    stmt.close()
    resultSet
  }

  /**
   * 根据产品id查询产品信息
   *
   * @param productId 产品id
   * @return
   */
  def selectByProductionId(productId: Int): ResultSet = {
    if (stmt == null) {
      init()
    }
    val sql: String = String.format("select * from product where product_id = %S", productId.toString)
    val resultSet: ResultSet = stmt.executeQuery(sql)
    connection.close()
    stmt.close()
    resultSet
  }

}
