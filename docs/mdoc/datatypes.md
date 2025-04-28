---
id: datatypes
title: Типы данных
---

Типы данных поддерживаемые Yupana

| Тип       |         Scala-тип         |           JDBC-тип            |        Нижняя граница         |        Верхняя граница        |
|-----------|:-------------------------:|:-----------------------------:|:-----------------------------:|:-----------------------------:|
| TIMESTAMP |   `org.yupana.api.Time`   |          `Timestamp`          | -292275055-05-16 16:47:04.192 | +292278994-08-17 07:12:55.807 |
| TINYINT   |          `Byte`           |            `Byte`             |             -128              |              127              |
| SMALLINT  |          `Short`          |            `Short`            |            -32768             |             32767             |
| INTEGER   |           `Int`           |           `Integer`           |          -2147483648          |          2147483647           | 
| BIGINT    |          `Long`           |            `Long`             |          $-{2}^{63}$          |         $2^{63} - 1$          |
| DOUBLE    |         `Double`          |           `Double`            |           $-\infty$           |           $\infty$            |
| DECIMAL   |       `BigDecimal`        |         `BigDecimal`          |              N/A              |              N/A              |
| CURRENCY  | `org.yupana.api.Currency` |         `BigDecimal`          |    $\frac{{-2}^{63}}{100}$    |  $\frac{{2}^{63} - 1}{100}$   |
| VARCHAR   |         `String`          |           `String`            |                               |                               |
| BLOB      |   `org.yupana.api.Blob`   | `org.yupana.jdbc.YupanaBlob`  |                               |                               |
| ARRAY     |        `Array[T]`         | `org.yupana.jdbc.YupanaArray` |                               |                               |

Кроме указанных типов поддерживаются кортежи арности 2, которые не могут быть использованы в проекциях, но применяются в условиях.