package com.rison.flink.client

import java.io.IOException

import com.rison.flink.common.Logger
import com.rison.flink.util.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put, Table}

/**
 * @author : Rison 2021/9/22 下午10:31
 *         Hbase 客户端
 */
object HbaseClient extends Logger {

  var admin: Admin = _
  var conn: Connection = _

  /**
   * 初始化
   */
  def init(): Unit = {
    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("hbase.rootdir", Property.getStrValue("hbase.rootdir"))
    configuration.set("hbase.zookeeper.quorum", Property.getStrValue("hbase.zookeeper.quorum"))
    configuration.set("hbase.client.scanner.timeout.period", Property.getStrValue("hbase.client.scanner.timeout.period"))
    configuration.set("hbase.rpc.timeout", Property.getStrValue("hbase.rpc.timeout"))
    try {
      conn = ConnectionFactory.createConnection(configuration)
      admin = conn.getAdmin
    } catch {
      case e: IOException => logger.warn("Hbase 初始化异常!错误信息为：{}", e.getMessage)
    }

  }

  /**
   * 写入数据
   *
   * @param tablename  表名
   * @param rowkey     行号
   * @param familyname 列簇名
   * @param column     字段
   * @param data       数据
   */
  def putData(tablename: String, rowkey: String, familyname: String, column: String, data: String) = {
    init()
    val table: Table = conn.getTable(TableName.valueOf(tablename))
    val put = new Put(rowkey.getBytes())
    put.addColumn(familyname.getBytes(), column.getBytes(), data.getBytes())
    table.put(put)
  }

}
