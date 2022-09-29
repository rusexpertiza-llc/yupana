---
id: kill
title: KILL
---

Команда KILL позволяет остановить выполняющиеся запросы.

Примеры:

Остановка запроса по идентификатору:

```sql
KILL QUERY WHERE QUERY_ID = ?
```

Остановка запросов по состоянию:

```sql
KILL QUERY WHERE STATE = 'RUNNING'
```
