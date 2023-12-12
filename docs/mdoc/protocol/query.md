---
id: query
title: Простой запрос
---

После успешной [авторизации](connect.md) сервер готов к получению [YupanaQL](../yupanaql.md) запросов.

Клиент отправляет запрос `SqlQuery` с идетнификатором, телом запроса и параметрами.  Идентификаторы запроса должны
быть уникальными в рамках одного соединения. Если запрос обработан успешно сервер отправляет `ResultHeader` с информацией
о данных. Клиент шлет команду `Next` с желаемым колличеством строк. Сервер отправляет строки в виде `ResultRow`. Если
данных больше нет отправляется `ResultFooter`.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    C->>S: q: SqlQuery(Id, YpQL, Map[Id -> ParameterValue])
    S->>C: H: ResultHeader(Id, TableName, Columns)
    loop has more data
      C->>S: n: Next(Id, Batch size)
      loop batch size
          S->>C: R: ResultRow(Id, Values)
      end
    end
    S->>C: F: ResultFooter(Id, Statistics)
```

Клиент может досрочно завершить выполнение запроса отправив команду `Cancel`.