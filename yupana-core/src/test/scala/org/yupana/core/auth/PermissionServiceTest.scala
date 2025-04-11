package org.yupana.core.auth

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PermissionServiceTest extends AnyFlatSpec with Matchers {
  "PermissionService" should "check permissions" in {
    val service = new PermissionService(putEnabled = true)

    val user1 = YupanaUser("Test1", None, TsdbRole.ReadOnly)
    service.hasPermission(user1, Object.Table(Some("items_kkm")), Action.Read) shouldBe true
    service.hasPermission(user1, Object.Table(Some("items_kkm")), Action.Write) shouldBe false

    service.hasPermission(user1, Object.User, Action.Read) shouldBe false
    service.hasPermission(user1, Object.User, Action.Write) shouldBe false

    service.hasPermission(user1, Object.Queries, Action.Read) shouldBe true
    service.hasPermission(user1, Object.Queries, Action.Write) shouldBe false

    service.hasPermission(user1, Object.Metadata, Action.Read) shouldBe true
    service.hasPermission(user1, Object.Metadata, Action.Write) shouldBe true

    val user2 = YupanaUser("Test1", None, TsdbRole.ReadWrite)
    service.hasPermission(user2, Object.Table(Some("items_kkm")), Action.Read) shouldBe true
    service.hasPermission(user2, Object.Table(Some("items_kkm")), Action.Write) shouldBe true

    service.hasPermission(user2, Object.User, Action.Read) shouldBe false
    service.hasPermission(user2, Object.User, Action.Write) shouldBe false

    service.hasPermission(user2, Object.Queries, Action.Read) shouldBe true
    service.hasPermission(user2, Object.Queries, Action.Write) shouldBe false

    service.hasPermission(user2, Object.Metadata, Action.Read) shouldBe true
    service.hasPermission(user2, Object.Metadata, Action.Write) shouldBe true

    val user3 = YupanaUser("admin", Some("12345"), TsdbRole.Admin)
    service.hasPermission(user3, Object.Table(Some("items_kkm")), Action.Read) shouldBe true
    service.hasPermission(user3, Object.Table(Some("items_kkm")), Action.Write) shouldBe true

    service.hasPermission(user3, Object.User, Action.Read) shouldBe true
    service.hasPermission(user3, Object.User, Action.Write) shouldBe true

    service.hasPermission(user3, Object.Queries, Action.Read) shouldBe true
    service.hasPermission(user3, Object.Queries, Action.Write) shouldBe true

    service.hasPermission(user3, Object.Metadata, Action.Read) shouldBe true
    service.hasPermission(user3, Object.Metadata, Action.Write) shouldBe true
  }

  it should "not allow write when put disabled" in {
    val service = new PermissionService(putEnabled = false)

    val user = YupanaUser("Test1", None, TsdbRole.ReadWrite)
    service.hasPermission(user, Object.Table(Some("items_kkm")), Action.Read) shouldBe true
    service.hasPermission(user, Object.Table(Some("items_kkm")), Action.Write) shouldBe false

    service.hasPermission(user, Object.User, Action.Read) shouldBe false
    service.hasPermission(user, Object.User, Action.Write) shouldBe false

    service.hasPermission(user, Object.Queries, Action.Read) shouldBe true
    service.hasPermission(user, Object.Queries, Action.Write) shouldBe false
  }
}
