package org.yupana.benchmarks

import io.prometheus.client.{CollectorRegistry, Summary}
import io.prometheus.client.exporter.PushGateway
import org.openjdk.jmh.runner.Runner
import org.openjdk.jmh.runner.options.CommandLineOptions

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object BenchmarksRunner {

  def main(args: Array[String]): Unit = {
    val params = RunnerParams(args).get
    val opts = new CommandLineOptions(args.filterNot(_.startsWith("--")): _*)
    val runner = new Runner(opts)
    val results = runner.run()
    val gateway = new PushGateway(params.pushGatewayUrl)

    results.asScala.foreach { result ⇒
      val benchmark = result.getParams.getBenchmark.replaceAll("\\.", "_")
      val score = result.getAggregatedResult.getPrimaryResult.getScore
      observe(benchmark, "score", score)
      result.getSecondaryResults.entrySet().asScala.foreach { r =>
        observe(benchmark,
          r.getValue.getLabel
            .replaceAll("·", "")
            .replaceAll("\\.", "_"),
          r.getValue.getScore)
      }
    }
    gateway.pushAdd(CollectorRegistry.defaultRegistry, "yupana_benchmarks_job")
  }

  private def observe(benchmark: String, name: String, value: Double): Unit = {
    val summary = Summary
      .build()
      .namespace("yupana_benchmarks")
      .name(benchmark + "_" + name)
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