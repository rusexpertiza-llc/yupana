/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.jdbc

import org.yupana.api.query.Result
import org.yupana.protocol.ParameterValue

import java.sql.SQLException
import java.util.Properties
import scala.concurrent.ExecutionContext

class YupanaConnectionImpl(override val url: String, properties: Properties) extends YupanaConnection {
  implicit private val ec: ExecutionContext = ExecutionContext.global

  private var closed = false

  private val tcpClient = new YupanaTcpClient(
    properties.getProperty("yupana.host"),
    properties.getProperty("yupana.port").toInt,
    Option(properties.getProperty("yupana.batchSize")).map(_.toInt).getOrElse(100),
    properties.getProperty("user"),
    properties.getProperty("password")
  )

  tcpClient.connect(System.currentTimeMillis())

  override def runQuery(query: String, params: Map[Int, ParameterValue]): Result = {
    try {
      tcpClient.prepareQuery(query, params)
    } catch {
      case e: Throwable =>
        throw new SQLException(e)
    }
  }

  override def runBatchQuery(query: String, params: Seq[Map[Int, ParameterValue]]): Result = {
    try {
      tcpClient.batchQuery(query, params)
    } catch {
      case e: Throwable =>
        throw new SQLException(e)
    }
  }

  @throws[SQLException]
  override def close(): Unit = {
    tcpClient.close()
    closed = true
  }

  @throws[SQLException]
  override def isClosed: Boolean = closed
}
