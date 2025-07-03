/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.benchmarks

import io.prometheus.metrics.core.metrics.Summary
import io.prometheus.metrics.exporter.pushgateway.PushGateway
import org.openjdk.jmh.runner.Runner
import org.openjdk.jmh.runner.options.CommandLineOptions

import scala.jdk.CollectionConverters._

/**
  * Entry point for pushing benchmark results into prometheus. Pass all incoming arguments into JMH benchmark
  * Example of usage:
  * sbt "benchmarks/jmh:runMain org.yupana.benchmarks.BenchmarksRunner --pushGatewayUrl=localhost:9091 .TSDHBaseRowIteratorBenchmark. -prof gc
  */
object BenchmarksRunner {

  def main(args: Array[String]): Unit = {
    val params = RunnerParams(args).get
    val opts = new CommandLineOptions(args.filterNot(_.startsWith("--")): _*)
    val runner = new Runner(opts)
    val results = runner.run()
    val gateway = PushGateway.builder().address(params.pushGatewayUrl).job("yupana_benchmarks_job").build()

    results.asScala.foreach { result =>
      val benchmark = result.getParams.getBenchmark.split("\\.").takeRight(2).mkString("_")
      val score = result.getAggregatedResult.getPrimaryResult.getScore
      observe(benchmark, "score", score)
      result.getSecondaryResults.entrySet().asScala.foreach { r =>
        observe(
          benchmark,
          r.getValue.getLabel
            .replaceAll("·", "")
            .replaceAll("\\.", "_"),
          r.getValue.getScore
        )
      }
    }
    gateway.push()
  }

  private def observe(benchmark: String, name: String, value: Double): Unit = {
    val summary = Summary
      .builder()
      .name("yupana_benchmarks_" + benchmark + "_" + name)
      .help("Yupana benchmark summary")
      .register()
    summary.observe(value)
  }
}

case class RunnerParams(pushGatewayUrl: String = null)

object RunnerParams {
  def apply(args: Array[String]): Option[RunnerParams] = {
    val parser = new scopt.OptionParser[RunnerParams]("RunnerParams") {
      override def errorOnUnknownArgument = false
      override def reportWarning(msg: String): Unit = {}

      help("help").text("Benchmarks runner")

      opt[String]("pushGatewayUrl")
        .action((v, p) => p.copy(pushGatewayUrl = v))
        .required()
        .text("Prometeus pushgateaway url")

    }
    parser.parse(args, RunnerParams())
  }
}
