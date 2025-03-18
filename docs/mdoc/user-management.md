---
id: user-management
title: Управление пользователями
---

## Создание пользователя

```sql
CREATE USER 'John'
  WITH PASSWORD '12345'
  WITH ROLE 'read_only'
```

Доступны 4 роли:

| Роль       | Таблицы | Метаданные | Пользователи | Запросы |
|------------|:-------:|:----------:|:------------:|:-------:|
| DISABLED   |    -    |     -      |      -       |    -    |
| READ_ONLY  |    R    |     R      |      -       |    R    |
| READ_WRITE |   RW    |     R      |      -       |    R    |
| ADMIN      |   RW    |     R      |      RW      |   RW    |

## Редактирование пользователя

```sql
ALTER USER 'John'
  SET PASSWORD='54321'
  SET ROLE='read_write'
```

Также можно менять только пароль или только роль.

## Удаление пользователя

```sql
DROP USER 'John'
```

## Просмотр пользователей

```sql
SHOW USERS
```
