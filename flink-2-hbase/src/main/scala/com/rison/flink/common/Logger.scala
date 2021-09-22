package com.rison.flink.common

import com.typesafe.scalalogging.Logger

/**
 * @author : Rison 2021/9/22 下午9:55
 *         日志打印工具
 */
trait Logger {
 val logger = Logger(this.getClass)
}
