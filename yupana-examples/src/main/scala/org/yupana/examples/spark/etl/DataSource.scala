package org.yupana.examples.spark.etl

import org.joda.time.LocalDateTime

import scala.util.Random

object DataSource {

  val goods = Array(
    "Колбаса вареная",
    "Колбаса соленая",
    "Сигареты крепкие",
    "Водка",
    "Селедка",
    "Карбюратор",
    "Топор туристический",
    "Штангенциркуль",
    "Медведь большой",
    "Салат цезарь"
  )

  val measures = Array(
    Some("кг"), Some("шт"), Some("л"), None
  )


  def getReceipts(n: Int): Seq[Receipt] = {
    1 to n map genReceipt
  }


  def genReceipt(n: Int): Receipt = {
    val items = 1 to (1 + Random.nextInt(10)) map (_ => genItem)
    val date = LocalDateTime.now.minusMillis(Random.nextInt(86400000)).minusDays(Random.nextInt(30))
    val ts = items.foldLeft(BigDecimal(0))(_ + _.sum)
    val (byCard, byCash) = if (Random.nextBoolean()) (Some(ts), None) else (None, Some(ts))

    Receipt(
      date = date,
      receiptNumber = n,
      operationType = if (Random.nextBoolean()) "BUY" else "SELL",
      operator = if (Random.nextBoolean()) "Иванов Иван" else "Петров Петр",
      shiftNumber = Random.nextInt(5),
      kkmId = Random.nextInt(20),
      items = items,
      totalSum = ts,
      totalCardSum = byCard,
      totalCashSum = byCash,
      taxes = Map.empty
    )
  }

  def genItem: Item = {
    Item(
      name = goods(Random.nextInt(goods.length)),
      sum = (Random.nextInt(1000) + 1) * 0.5,
      quantity = Random.nextDouble() * 10,
      measure = measures(Random.nextInt(measures.length)),
      taxes = Map.empty
    )
  }
}
