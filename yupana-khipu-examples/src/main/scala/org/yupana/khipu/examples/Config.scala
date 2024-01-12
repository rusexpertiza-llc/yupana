package org.yupana.khipu.examples

import org.yupana.settings.Settings

import java.util.Properties
import scala.jdk.CollectionConverters._

case class Config(
    host: String = "localhost",
    port: Int = 12345,
    settings: Settings
)

object Config {
  def create(cfg: com.typesafe.config.Config): Config = {
    val props = new Properties()
    cfg.entrySet().asScala.foreach(x => props.setProperty(x.getKey, x.getValue.unwrapped().toString))

    val settings = Settings(props)

    Config(
      cfg.getString("yupana.host"),
      cfg.getInt("yupana.port"),
      settings
    )
  }
}
