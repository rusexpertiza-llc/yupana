---
id: kill
title: KILL
---

Комманда KILL позволяет остановить выполняющиеся запросы.

Примеры:

Остановка запроса по идентификатору:

`KILL QUERY WHERE QUERY_ID = ?`

Остановка запросов по состоянию:

`KILL QUERY WHERE STATE = 'RUNNING'`
