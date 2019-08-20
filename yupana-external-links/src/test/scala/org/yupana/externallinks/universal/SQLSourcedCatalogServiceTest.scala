package org.yupana.externallinks.universal

import java.util.Properties

import org.flywaydb.core.Flyway
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, OptionValues}
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.yupana.core.cache.CacheFactory
import org.yupana.externallinks.universal.JsonCatalogs.{SQLExternalLink, SQLExternalLinkConfig}
import org.yupana.schema.{Dimensions, SchemaRegistry}

class SQLSourcedCatalogServiceTest extends FlatSpec with Matchers with OptionValues with BeforeAndAfterAll{
  val dbUrl = "jdbc:h2:mem:yupana;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
  val dbUser = "test"
  val dbPass = "secret"


  private def createService(config: SQLExternalLinkConfig): SQLSourcedExternalLinkService = {
    val ds = new DriverManagerDataSource(config.connection.url, config.connection.username.orNull, config.connection.password.orNull)
    val jdbc = new JdbcTemplate(ds)

    val externalLink = SQLExternalLink(config, Dimensions.KKM_ID_TAG)

    new SQLSourcedExternalLinkService(externalLink, config.description, jdbc, null)
  }


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
      case _ =>
    }
    val catalog = parsed.right.get.head
    val catalogService = createService(catalog)

    val values = catalogService.fieldValuesForDimValues(catalog.description.fieldsNames, Set("123432345655", "123432345657"))

    values.get("123432345655", "f1").value shouldEqual "wer"
    values.get("123432345655", "f2").value shouldEqual "sdf"
    values.get("123432345657", "f1").value shouldEqual "rty"
    values.get("123432345657", "f2").value shouldEqual "fgh"

    val tagsForAll = catalogService.dimValuesForAllFieldsValues(Seq(
      ("f1", Set("qwe", "ert")),
      ("f2", Set("asd", "fgh")))
    )

    tagsForAll should contain theSameElementsAs Set("123432345654")

    val tagsForAny = catalogService.dimValuesForAnyFieldsValues(Seq(
      ("f1", Set("qwe", "ert")),
      ("f2", Set("asd", "fgh")))
    )

    tagsForAny should contain theSameElementsAs Set("123432345654", "123432345656", "123432345657")
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
    val catalog = JsonExternalLinkDeclarationsParser.parse(SchemaRegistry.defaultSchema, complicatedCatalogJson).right.get.head
    val catalogService = createService(catalog)

    val values = catalogService.fieldValuesForDimValues(catalog.description.fieldsNames, Set("123432345655", "123432345657"))

    values.get("123432345655", "f1").value shouldEqual "hhh2"
    values.get("123432345655", "f2").value shouldEqual "ggg"
    values.get("123432345657", "f1").value shouldEqual "hhh3"
    values.get("123432345657", "f2").value shouldEqual "ggg3"

    val tagsForAll = catalogService.dimValuesForAllFieldsValues(Seq(
      ("f1", Set("hhh", "hhh3")),
      ("f2", Set("ggg2", "ggg3")))
    )

    tagsForAll should contain theSameElementsAs Set("123432345657")

    val tagsForAny = catalogService.dimValuesForAnyFieldsValues(Seq(
      ("f1", Set("hhh", "hhh3")),
      ("f2", Set("ggg2", "ggg3")))
    )

    tagsForAny should contain theSameElementsAs Set("123432345654", "123432345656", "123432345657")
  }

  override protected def beforeAll(): Unit = {
    val flyway = new Flyway()
    flyway.setDataSource(dbUrl, dbUser, dbPass)
//    flyway.setLocations("")
    flyway.migrate()

    val props = new Properties()
    props.put("analytics.caches.default.engine", "EhCache")
    props.put("analytics.caches.UniversalCatalogFieldValuesCache.maxElements", "100")
    props.put("analytics.caches.UniversalCatalogFieldValuesCache.heapSize", "1024")
    props.put("analytics.caches.UniversalCatalogFieldValuesCache.offHeapSize", "0")
    CacheFactory.init(props, "ns")
  }
}
