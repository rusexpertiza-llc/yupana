---
id: show
title: SHOW
---

Команда SHOW позволят получать различную метаинформацию.

## SHOW TABLES

```sql
SHOW TABLES
```
выдает информацию обо всех имеющихся таблицах.

## SHOW COLUMNS

```sql
SHOW COLUMNS FROM <table_name>
```
выдает информацию о полях указанной таблицы.

## SHOW QUERIES

```sql
SHOW QUERIES
```
выдает информацию об истории запросов.

```sql
SHOW QUERIES [WHERE (QUERY_ID|STATE) = ?] [limit n];
```

Где:
  - `QUERY_ID` - возвращается набор метрик для конкретного запроса.
  - `STATE` - возвращаются все наборы метрик для запросов в указанном статусе.
  - `limit` ограничивает размер результата, где `n` - целое число

## SHOW UPDATES_INTERVALS

Данная команда позволяет получить информацию о состоянии хранимых сверток:
  1. Информация об актуальности сверток по периодам.
  2. Информация о пересчете сверток.

```sql
SHOW UPDATES_INTERVALS
  WHERE
    TABLE = '<tableName>' AND
    UPDATED_AT BETWEEN <FROM> AND <TO> AND
    RECALCULATED_AT BETWEEN <FROM> AND <TO>
```

Где:

- `tableName` - имя таблицы, в которую свертка складывает данные
- `UPDATED_AT` - фильтр по дате завершения свертки, где `FROM` - начало периода, `TO` - конец периода
- `RECALCULATED_AT` - фильтр по датам, за которые сверткой были пересчитаны данные, где `FROM` - начало периода, `TO` - конец периода
