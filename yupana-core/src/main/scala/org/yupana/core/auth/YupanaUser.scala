package org.yupana.core.auth

case class YupanaUser(name: String)

object YupanaUser {

  val ANONYMOUS: YupanaUser = YupanaUser("ANONYMOUS")
}
