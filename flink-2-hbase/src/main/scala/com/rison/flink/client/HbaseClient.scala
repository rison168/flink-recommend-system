package com.rison.flink.client

import java.io.IOException

import com.rison.flink.common.Logger
import com.rison.flink.util.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Get, Put, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer

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
   * @param tableName  表名
   * @param rowKey     行号
   * @param familyName 列簇名
   * @param column     字段
   * @param data       数据
   */
  def putData(tableName: String, rowKey: String, familyName: String, column: String, data: String) = {
    if (conn == null) {
      init()
    }
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val put = new Put(rowKey.getBytes())
    put.addColumn(familyName.getBytes(), column.getBytes(), data.getBytes())
    table.put(put)
    conn.close()
  }

  /**
   * 读取数据
   *
   * @param tableName
   * @param rowKey
   * @param familyName
   * @param column
   * @return
   */
  def getData(tableName: String, rowKey: String, familyName: String, column: String): String = {
    if (conn == null) {
      init()
    }
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val row: Array[Byte] = Bytes.toBytes(rowKey)
    val get = new Get(row)
    val result: Result = table.get(get)
    val resultValue: Array[Byte] = result.getValue(familyName.getBytes(), column.getBytes())
    if (resultValue == null) null
    val str = new String(resultValue)
    conn.close()
    str
  }

  /**
   * 单元字段元素记录数据 加1
   *
   * @param tableName  表名
   * @param rowKey     行号
   * @param familyName 列簇名
   * @param column     列名
   */
  def increaseColumn(tableName: String, rowKey: String, familyName: String, column: String): Unit = {
    if (conn == null) {
      init()
    }
    val str: String = getData(tableName, rowKey, familyName, column)
    var res: Int = 1;
    if (str != null) {
      res = str.toInt + 1
    }
    putData(tableName, rowKey, familyName, column, res.toString)
    conn.close()
  }

  /**
   * 取出表的所有key
   *
   * @param tableName
   * @return
   */
  def getAllKey(tableName: String): List[String] = {
    if (conn == null) {
      init()
    }
    val keys: ListBuffer[String] = ListBuffer[String]()
    val scan = new Scan()
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val scanner: ResultScanner = table.getScanner(scan)
    scanner.forEach(
      r => {
        keys.append(r.getRow.toString)
      }
    )
    conn.close()
    keys.toList
  }

  /**
   * 获取一行的所有数据 并且排序
   * @param tableName
   * @param rowKey
   * @return
   */
  def getRow(tableName: String, rowKey: String): List[(String, Double)] = {
    if (conn != null) {
      init()
    }
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val row: Array[Byte] = Bytes.toBytes(rowKey)
    val get = new Get(row)
    val result: Result = table.get(get)
    val list = ListBuffer[(String, Double)]()
    result.listCells().forEach(
      cell => {
        val key: String = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
        val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        list.append((key, value.toDouble))
      }
    )
    conn.close()
    list.toList.sortWith(_._2 > _._2)
  }

}
