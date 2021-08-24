package org.yupana.externallinks.universal

import org.flywaydb.core.Flyway
import org.h2.jdbcx.JdbcDataSource
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ BeforeAndAfterAll, OptionValues }
import org.yupana.api.query.Replace
import org.yupana.core.cache.CacheFactory
import org.yupana.externallinks.TestSchema
import org.yupana.externallinks.universal.JsonCatalogs.{ SQLExternalLink, SQLExternalLinkConfig }
import org.yupana.schema.{ Dimensions, SchemaRegistry }

import java.util.Properties

class SQLSourcedCatalogServiceTest extends AnyFlatSpec with Matchers with OptionValues with BeforeAndAfterAll {
  val dbUrl = "jdbc:h2:mem:yupana;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
  val dbUser = "test"
  val dbPass = "secret"

  private def createService(config: SQLExternalLinkConfig): SQLSourcedExternalLinkService[Int] = {
    val ds = new JdbcDataSource()
    ds.setURL(config.connection.url)
    ds.setUser(config.connection.username.orNull)
    ds.setPassword(config.connection.password.orNull)

    val externalLink = SQLExternalLink[Int](config, Dimensions.KKM_ID)

    new SQLSourcedExternalLinkService(TestSchema.schema, externalLink, config.description, ds)
  }

  import org.yupana.api.query.syntax.All._

  "SQLSourcedCatalog" should "work as defined in json" in {

    val simpleCatalogJson = s"""{
              |  "externalLinks": [
              |   {
              |    "description" : {
              |      "source": "sql",
              |      "linkName": "TestCatalog",
              |      "dimensionName": "kkmId",
              |      "fieldsNames": ["f1", "f2"],
              |      "tables": ["receipt"]
              |    },
              |    "connection" : {
              |      "url": "$dbUrl",
              |      "username": "$dbUser",
              |      "password": "$dbPass"
              |    }
              |   }
              |  ]
              |}""".stripMargin
    val parsed = JsonExternalLinkDeclarationsParser.parse(SchemaRegistry.defaultSchema, simpleCatalogJson)
    parsed match {
      case Left(err) => throw new TestFailedException(err, 3)
      case _         =>
    }
    val externalLinkConfig = parsed.right.get.head
    val externalLinkService = createService(externalLinkConfig)
    val externalLink = externalLinkService.externalLink

    val values =
      externalLinkService.fieldValuesForDimValues(
        externalLinkConfig.description.fieldsNames,
        Set(12345655, 12345657)
      )

    values.get(12345655, "f1").value shouldEqual "wer"
    values.get(12345655, "f2").value shouldEqual "sdf"
    values.get(12345657, "f1").value shouldEqual "rty"
    values.get(12345657, "f2").value shouldEqual "fgh"

    val c1 = in(lower(link(externalLink, "f1")), Set("qwe", "ert"))
    val c1_2 = in(lower(link(externalLink, "f2")), Set("asd", "fgh"))
    val inCondition = externalLinkService.transform(
      and(
        c1,
        c1_2
      )
    )
    inCondition shouldEqual Seq(
      Replace(
        Set(c1, c1_2),
        in(dimension(externalLink.dimension.aux), Set(12345654))
      )
    )

    val c2 = notIn(lower(link(externalLink, "f1")), Set("qwe", "ert"))
    val c2_2 = notIn(lower(link(externalLink, "f2")), Set("asd", "fgh"))
    val notInCondition = externalLinkService.transform(
      and(
        c2,
        c2_2
      )
    )

    notInCondition shouldEqual Seq(
      Replace(
        Set(c2, c2_2),
        notIn(
          dimension(externalLink.dimension.aux),
          Set(12345654, 12345656, 12345657)
        )
      )
    )
  }

  it should "work from json definition with custom fields mapping and relation" in {
    val complicatedCatalogJson = s"""{
                               |  "externalLinks": [
                               |    {
                               |     "description" : {
                               |      "source": "sql",
                               |      "linkName": "ComplicatedTestCatalog",
                               |      "dimensionName": "kkmId",
                               |      "fieldsNames": ["f1", "f2"],
                               |      "fieldsMapping": {
                               |        "kkmId": "t1.k",
                               |        "f1": "t1.ff1",
                               |        "f2": "t2.ff2"
                               |      },
                               |      "relation": "t1 JOIN t2 ON t1.t2_id = t2.id",
                               |      "tables": ["receipt"]
                               |     },
                               |     "connection" : {
                               |      "url": "$dbUrl",
                               |      "username": "$dbUser",
                               |      "password": "$dbPass"
                               |     }
                               |    }
                               |  ]
                               |}""".stripMargin
    val externalLinkConfig =
      JsonExternalLinkDeclarationsParser.parse(SchemaRegistry.defaultSchema, complicatedCatalogJson).right.get.head
    val externalLinkService = createService(externalLinkConfig)
    val externalLink = externalLinkService.externalLink

    val values =
      externalLinkService.fieldValuesForDimValues(
        externalLinkConfig.description.fieldsNames,
        Set(12345655, 12345657)
      )

    values.get(12345655, "f1").value shouldEqual "hhh2"
    values.get(12345655, "f2").value shouldEqual "ggg"
    values.get(12345657, "f1").value shouldEqual "hhh3"
    values.get(12345657, "f2").value shouldEqual "ggg3"

    val c1 = in(lower(link(externalLink, "f1")), Set("hhh", "hhh3"))
    val c1_2 = in(lower(link(externalLink, "f2")), Set("ggg2", "ggg3"))
    val inCondition = externalLinkService.transform(
      and(
        c1,
        c1_2
      )
    )

    inCondition shouldEqual Seq(
      Replace(
        Set(c1, c1_2),
        in(dimension(externalLink.dimension.aux), Set(12345657))
      )
    )

    val c2 = notIn(lower(link(externalLink, "f1")), Set("hhh", "hhh3"))
    val c2_2 = notIn(lower(link(externalLink, "f2")), Set("ggg2", "ggg3"))
    val notInCondition = externalLinkService.transform(
      and(
        c2,
        c2_2
      )
    )

    notInCondition shouldEqual Seq(
      Replace(
        Set(c2, c2_2),
        notIn(
          dimension(externalLink.dimension.aux),
          Set(12345654, 12345656, 12345657)
        )
      )
    )
  }

  override protected def beforeAll(): Unit = {
    val flyway = Flyway.configure().dataSource(dbUrl, dbUser, dbPass).load()
    flyway.migrate()

    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(props, "ns")
  }

  import JsonExternalLinkCachingTest._

  override protected def afterAll(): Unit = {
    resetCacheFactory()
  }
}
