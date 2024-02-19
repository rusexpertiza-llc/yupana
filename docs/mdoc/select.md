---
id: select
title: SELECT
---

Операция SELECT служит для выборки данных. Запрос состоит из:

 - списка полей для выборки
 - имени таблицы
 - условия
 - списка полей для группировки (не обязательно)
 - условия фильтрации по сгруппированным данным (не обязательно)

> Условие является обязательным и должно содержать как минимум временной интервал (`time > x AND time < y`).
> Если условие содержит выражение `OR` то временной интервал должен быть определен для каждого из подусловий.

## Примеры запросов

Суммы продаж для указанной кассы за указанный период с разбивкой по дням:
```sql
SELECT sum(sum), day(time) as d, kkmId
  FROM items_kkm
  WHERE time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01' AND kkmId = '10'
  GROUP BY d, kkmId
```

Суммы продаж товаров в которых встречается слово "штангенциркуль" за указанный период с разбивкой по дням:
```sql
SELECT 
    sum(sum), day(time) as d, kkmId
FROM 
    items_kkm
WHERE 
    time >= TIMESTAMP '2019-06-01' AND 
    time < TIMESTAMP '2019-07-01' AND 
    itemsInvertedIndex_phrase = 'штангенциркуль'
GROUP BY 
    d, kkmId
```

Время первой и последней продажи селедки за сутки:

```sql
SELECT 
    min(time) as mint, max(time) as maxt, day(time) as d
FROM items_kkm
WHERE 
    time >= TIMESTAMP '2019-06-01' AND 
    time < TIMESTAMP '2019-07-01' AND
    itemsInvertedIndex_phrase = 'селедка'
GROUP BY d
```

Считаем количество продаж товаров, купленных в количестве больше 10:

```sql
SELECT 
    item, 
    sum(
    CASE
        WHEN quantity > 9 THEN 1
        ELSE 0 
    )
FROM items_kkm
WHERE 
    time >= TIMESTAMP '2019-06-01'
    AND time < TIMESTAMP '2019-07-01'
GROUP BY item
```

Применяем фильтры после расчета оконной функции:

```sql
SELECT
  kkmId,
  time AS t,
  lag(time) AS l
FROM receipt
WHERE time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01'
GROUP BY kkmId
HAVING
  ((l - t) > INTERVAL '2' HOUR AND extract_hour(t) >= 8 AND extract_hour(t) <= 18) OR
  ((l - t) > INTERVAL '4' HOUR AND extract_hour(t) > 18 OR extract_hour(t) < 8)
```

Выбираем предыдущие три месяца:
```sql
SELECT sum(sum), day(time) as d, kkmId
FROM items_kkm
WHERE time >= trunc_month(now() - INTERVAL '3' MONTH) AND time < trunc_month(now())
GROUP BY d, kkmId
```

Агрегация по выражению:
```sql
SELECT kkmId,
    (CASE WHEN totalReceiptCardSum > 0 THEN 1 ELSE 0) as paymentType
FROM items_kkm
WHERE time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01'
GROUP BY paymentType, kkmId
```

Используем арифметику (`+`, `-`, `*`, `/`):
```sql
SELECT sum(totalSum) as ts, sum(cardSum) * max(cashSum) / 2 as something
FROM receipt
WHERE 
    time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01' AND
    kkmId = '11'
GROUP BY kkmId
```

Группируем колбасу по вкусу и считаем сумму:
```sql
SELECT
    item,
    case
      when contains_any(stem(item), stem('вареная')) then 'вареная'
      when contains_any(stem(item), stem('соленая')) then 'соленая'
      else 'невкусная' as taste,
    sum(sum)
FROM items_kkm
WHERE 
    time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01' 
    AND itemsInvertedIndex_phrase = 'колбаса'
GROUP BY item, taste
```

Использование ИЛИ для выборки по нескольким временным отрезкам:
```sql
SELECT trunc_day(time) as day, item, sum(sum) as sum
  FROM items_kkm
  WHERE ItemsInvertedIndex_phrase = 'лук' AND (
        (time >= TIMESTAMP '2021-08-01' AND time < TIMESTAMP '2021-08-08') OR
        (time >= TIMESTAMP '2022-08-01' AND time < TIMESTAMP '2022-08-08')
      )
  GROUP BY day, item
```

Использование ИЛИ для выборки по нескольким условиям:
```sql
SELECT trunc_day(time) as day, item, sum(sum) as sum
  FROM items_kkm
  WHERE (
          (ItemsInvertedIndex_phrase = 'лук' AND kkmId IN (1,2,3)) OR
          (itemsInvertedIndex_phrase = 'чеснок' AND kkmId IN (4,5,6))
        ) AND time >= TIMESTAMP '2022-08-01' AND time < TIMESTAMP '2022-08-08'
  GROUP BY day, item
```

Использование null:

```sql
SELECT
  trunc_day(time) as day,
  item,
  avg(CASE
    WHEN quantity > 0 then sum / quantity
    ELSE null
  ) AS avg_price
FROM kkm_items
WHERE time >= TIMESTAMP '2023-06-01' AND time < TIMESTAMP '2023-07-01'
```
