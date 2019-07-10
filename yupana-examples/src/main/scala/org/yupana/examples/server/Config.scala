package org.yupana.examples.server

import java.util.Properties
import com.typesafe.config.{Config => TypesafeConfig}

import scala.collection.JavaConverters._

case class Config(hbaseZookeeperUrl: String,
                  hbaseNamespace: String,
                  host: String = "localhost",
                  port: Int = 12345,
                  properties: Properties
                 )

object Config {
  def create(cfg: TypesafeConfig): Config = {
    val props = new Properties()
    cfg.entrySet().asScala.foreach(x => props.setProperty(x.getKey, x.getValue.unwrapped().toString))

    val namespace = if (cfg.hasPath("yupana.hbaseNamespace")) cfg.getString("yupana.hbaseNamespace") else "default"

    Config(
      cfg.getString("yupana.hbaseUrl"),
      namespace,
      cfg.getString("yupana.host"),
      cfg.getInt("yupana.port"),
      props
    )
  }
}
