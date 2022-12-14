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

package org.yupana.examples.server

import java.util.Properties
import com.typesafe.config.{ Config => TypesafeConfig }
import org.yupana.core.settings.Settings

import scala.jdk.CollectionConverters._

case class Config(
    hbaseZookeeperUrl: String,
    hbaseNamespace: String,
    host: String = "localhost",
    port: Int = 12345,
    settings: Settings
)

object Config {
  def create(cfg: TypesafeConfig): Config = {
    val props = new Properties()
    cfg.entrySet().asScala.foreach(x => props.setProperty(x.getKey, x.getValue.unwrapped().toString))

    val settings = Settings(props)
    val namespace = if (cfg.hasPath("yupana.hbaseNamespace")) cfg.getString("yupana.hbaseNamespace") else "default"

    Config(
      cfg.getString("yupana.hbaseUrl"),
      namespace,
      cfg.getString("yupana.host"),
      cfg.getInt("yupana.port"),
      settings
    )
  }
}
