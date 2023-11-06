package org.yupana.jdbc

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager

class DriverTest extends AnyFlatSpec with Matchers {

  "YupanaDriver" should "connect using properly" in {
    val server = new ServerMock

    val url = s"jdbc:yupana://127.0.0.1:${server.port}"
    val conn = DriverManager.getConnection(url, "test_user", "12345")

    conn.close()
  }

}
