---
id: delete
title: DELETE
---

Комманда DELETE позволяет удалять метрики запросов из истории.

Примеры:

Удаление метрики из истории по идентификатору запроса:

`DELETE QUERIES WHERE QUERY_ID = ?`

Удаление метрик по состоянию запросов:

`DELETE QUERIES WHERE STATE = 'FINISHED' `
