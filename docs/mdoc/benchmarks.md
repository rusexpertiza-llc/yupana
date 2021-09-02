---
id: benchmarks
title: Бенчмарки
---

Для измерения производительности используются бенчмарки на основе JMH. Их можно запустить командой `sbt benchmarks/jmh:run`

Также есть возможность отправить результаты бенчмарков в Prometheus для их последующего анализа и мониторинга.
Отправка результатов бенчмарка выполняется вариациями команды `sbt "benchmarks/jmh:runMain org.yupana.benchmarks.BenchmarksRunner --pushGatewayUrl={prometheus_pushgateway_url}`, аргументами которой могут быть любые стандартные параметры JMH

Например запуск конкретного бенчмарка `TSDHBaseRowIteratorBenchmark` с дополнительным профайлером:

`sbt "benchmarks/jmh:runMain org.yupana.benchmarks.BenchmarksRunner --pushGatewayUrl={prometheus_pushgateway_url} .*TSDHBaseRowIteratorBenchmark.* -prof gc"`