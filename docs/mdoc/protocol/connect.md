---
id: connect
title: Установка соединения
---

Процесс установки соединения начинается с отправки клиентом комманды `Hello`.  Сервер проверяет поддерживается ли
запрашиваемая версия протокола и в случае успеха отправляет `HelloResponse`. После этого сервер запрашивает данные для
авторизации `CredentialsRequest`.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    C->>S: h: Hello(protocol version, timestamp)
    S->>C: H: HelloResponse(protocol version, timestamp)
    S->>C: С: CredentialsRequest(method)
    C->>S: c: Credentials(method, user, password)
    S->>C: A: Authorized()
```

Если в процессе установки соединения происходит ошибка сервер посылает сообщение `ErrorMessage` и закрывает соединение.