package org.yupana.core.utils.metric

object MetricUtils {

  implicit class SavedMetrics[T](func: => T) {
    def withSavedMetrics(metricCollector: MetricQueryCollector): T =
      try func
      catch {
        case throwable: Throwable =>
          metricCollector.queryStatus.lazySet(Failed(throwable))
          metricCollector.finish()
          throw throwable
      }
  }
}
