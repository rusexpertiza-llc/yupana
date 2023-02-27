---
id: functions
title: Функции
---

## Типы функций

- Агрегация -- функция вычисляющая общее значение из множества значений (например сумму или максимум).  Агрегации не могут использоваться вместе с оконными функциями.
- Оконная -- функция вычисляющая общее значение из множества значении и их порядка. Оконные функции не могут использоваться вместе с агрегациями. Не поддерживаются в реализации Yupana для Spark.
- Унарная -- функция над одним значением (например `length` или `tokens`).
- Инфиксная -- функция над двумя значениями, в SQL записывается между аргументами (например + или -).
- Бинарная -- функция с двумя значениями, например `contains_all`.

## Поддерживаемые функции

| Функция                        | Тип функции   |     Типы аргументов     |  Тип значения   | Описание                                                                                                           |
|--------------------------------|:-------------:|:-----------------------:|:---------------:|--------------------------------------------------------------------------------------------------------------------|
| `min`                          | агрегация     |  число, строка, время   |     тот же      | Минимальное значение. Для строковых значение в лексикографическом порядке                                          |
| `max`                          | агрегация     |  число, строка, время   |     тот же      | Максимальное значение. Для строковых значение в лексикографическом порядке                                         |
| `sum`                          | агрегация     |          число          |     тот же      | Сумма                                                                                                              |
| [avg](#func_avg)               | агрегация     |          число          |   BigDecimal    | Среднее значение                                                                                                   |
| [count](#func_count)           | агрегация     |          любой          |      Long       | Количество                                                                                                         |
| [distinct_count](#func_dcount) | агрегация     |          любой          |      Long       | Количество уникальных значений                                                                                     |
| [hll_count](#func_hcount)      | агрегация     |  число, строка, время   |      Long       | Апроксимированное количество уникальных значений                                                                   |
| `lag`                          | оконная       |          любой          |     тот же      | Предыдущее значение в группе записей.  Группа определяется в запросе в секции группировки.  Сортировка по времени. |
| `trunc_year`                   | унарная       |          время          |      время      | Округление времени до года                                                                                         |
| `trunc_month`                  | унарная       |          время          |      время      | Округление времени до месяца                                                                                       |
| `trunc_day`                    | унарная       |          время          |      время      | Округление времени до дня                                                                                          |
| `trunc_hour`                   | унарная       |          время          |      время      | Округление времени до часа                                                                                         |
| `trunc_minute`                 | унарная       |          время          |      время      | Округление времени до минуты                                                                                       |
| `trunc_second`                 | унарная       |          время          |      время      | Округление времени до секунды                                                                                      |
| `exract_year`                  | унарная       |          время          |      число      | Извлечение значения года из времени                                                                                |
| `exract_month`                 | унарная       |          время          |      число      | Извлечение значения месяца из времени                                                                              |
| `exract_day`                   | унарная       |          время          |      число      | Извлечение значения дня из времени                                                                                 |
| `exract_hour`                  | унарная       |          время          |      число      | Извлечение значения часа из времени                                                                                |
| `exract_minute`                | унарная       |          время          |      число      | Извлечение значения минуты из времени                                                                              |
| `exract_second`                | унарная       |          время          |      число      | Извлечение значения секунды из времени                                                                             |
| `abs`                          | унарная       |          число          |      число      | Значение числа по модулю                                                                                           |
| `tokens`                       | унарная       |         строка          |  массив строк   | Получение стемированых транслитерированых строк из строки                                                          |
| `tokens`                       | унарная       |      массив строк       |  массив строк   | Получение стемированых транслитерированых строк из массива строк                                                   |
| `split`                        | унарная       |         строка          |  массив строк   | Разбиение строки на слова по пробелам                                                                              |
| `length`                       | унарная       |     строки, массивы     | строки, массивы | Длина строки или количество элементов в массиве                                                                    |
| `array_to_string`              | унарная       |         массив          |     строка      | Преобразование массивы в строку в формате "( a, b, .., n)"                                                         |
| `id`                           | унарная       |       размерность       |      число      | Идентификатор значения размерности в словаре                                                                       |
| `+`                            | инфиксная     | число, строка, интервал |     тот же      | Сложение                                                                                                           |
| `-`                            | инфиксная     |          число          |     тот же      | Вычитание                                                                                                          |
| `*`                            | инфиксная     |          число          |     тот же      | Умножение                                                                                                          |
| `/`                            | инфиксная     |          число          |     тот же      | Деление                                                                                                            |
| `+`                            | инфиксная     |    время и интервал     |      время      | Сложение                                                                                                           |
| `-`                            | инфиксная     |    время и интервал     |      время      | Вычитание                                                                                                          |
| `-`                            | инфиксная     |      время и время      |    интервал     | Вычитание                                                                                                          |
| `=`                            | инфиксная     |  число, строка, время   |   логический    | Сравнение на равенство                                                                                             |
| `<>` или `!=`                  | инфиксная     |  число, строка, время   |   логический    | Сравнение на неравенство                                                                                           |
| `>`                            | инфиксная     |  число, строка, время   |   логический    | Сравнение на больше                                                                                                |
| `<`                            | инфиксная     |  число, строка, время   |   логический    | Сравнение на меньше                                                                                                |
| `>`=                           | инфиксная     |  число, строка, время   |   логический    | Сравнение на больше или равно                                                                                      |
| `<=`                           | инфиксная     |  число, строка, время   |   логический    | Сравнение на меньше или равно                                                                                      |
| `contains`                     | бинарная      |  массив и тип элемента  |   логический    | True если массив содержит элемент, иначе False                                                                     |
| `contains_all`                 | бинарная      |     массив и массив     |   логический    | True если массив1 содержит все элементы массива2, иначе False                                                      |
| `contains_any`                 | бинарная      |     массив и массив     |   логический    | True если массив1 содержит хотя бы один элемент из массива2, иначе False                                           |
| `contains_same`                | бинарная      |     массив и массив     |   логический    | True если массив1 содержит те же элементы что и массив2 (в любом порядке)                                          |

### Функция avg
Функция `avg` представляет собой функцию агрегации, позволяющую вычислять среднее значение для поля (metric, dimension) и возвращающяя значение типа BigDecimal.

Синтаксис:
```sql
avg(column_name)
```
Ограничения и особенности:
- Опеделена для следующих типов данных: Double, BigDecimal, Long, Int, BigInt, Short
- Игнорирует строки, в которых поле принимает значение NULL.
- Возвращает `null` в случае, если все значения поля в выборке имеют значение `null`.

Пример:
```sql
SELECT avg(column_name), id
  FROM some_table
  WHERE time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01'
  GROUP BY id
```

### Функция count
Функция `count` представляет собой функцию агрегации, позволяющую получать колличество строк в выборке для поля (metric, dimension) и возвращающяя значение типа Long.

Синтаксис:
```sql
count(column_name)
```

Ограничения и особенности:
- Опеделена для всех типов данных
- Игнорирует строки, в которых поле принимает значение NULL
- Возвращает `0` в случае, если все значения поля в выборке имеют значение `null`.

Пример:
```sql
SELECT count(column_name), id
  FROM some_table
  WHERE time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01'
  GROUP BY id
```

### Функция distinct_count
Функция `distinct_count` представляет собой функцию агрегации, позволяющую получать колличество строк с уникальным значением в выборке для поля (metric, dimension) и возвращающяя значение типа Long.

Синтаксис:
```sql
distinct_count(column_name)
```

Ограничения и особенности:
- Опеделена для всех типов данных
- Игнорирует строки, в которых поле принимает значение NULL
- Возвращает `0` в случае, если все значения поля в выборке имеют значение `null`.

Пример:
```sql
SELECT distinct_count(column_name), id
  FROM some_table
  WHERE time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01'
  GROUP BY id
```

### Функция hll_count
Функция `hll_count` представляет собой функцию агрегации, позволяющую получать апроксимированное колличество строк с уникальным значением в выборке для поля (metric, dimension) и возвращающяя значение типа Long.

Синтаксис:
```sql
hll_count(column_name, std_err)
```
- column_name - имя поля табницы
- std_err - стандартная ошибка. Представляет собой допустмое отклонение от истинного числа уникальных элементов (например, 0.18 - это допустимая ошибка в 18%). Реализация позволяет использовать ошибку в диапазоне от 0.00003 до 0.367.

Ограничения и особенности:
- Определена для целочисленных типов данных, строк и даты/времени
- Игнорирует строки, в которых поле принимает значение NULL
- Возвращает `0` в случае, если все значения поля в выборке имеют значение `null`.

Пример:
```sql
SELECT hll_count(column_name, 0.18), id
  FROM some_table
  WHERE time >= TIMESTAMP '2019-06-01' AND time < TIMESTAMP '2019-07-01'
  GROUP BY id
```

Кроме того, поддерживаются следующие SQL выражения:

| Выражение                 | Описание                                                                      |
|---------------------------|-------------------------------------------------------------------------------|
| `x IN (1, 2 .. z)`        | Проверка что `x` является одним из элементов заданного множества констант     |
| `x NOT IN (3, 4, .. z)`   | Проверка что `x` не является одним из элементов заданного множества констант  |
| `x IN NULL`               | Проверка что значение `x` не определено                                       |
| `x IS NOT NULL`           | Проверка что значение `x` определено                                          |
| `x BETWEEN 1 AND 10`      | То же самое что `x >= 1 AND x <= 10`                                          |