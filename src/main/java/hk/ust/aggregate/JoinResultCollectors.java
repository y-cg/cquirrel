package hk.ust.aggregate;

import hk.ust.aju.JoinResult;
import hk.ust.metrics.FlinkOperatorTimings;

/** Helpers for emitting {@link JoinResult} records with metrics accounting. */
public final class JoinResultCollectors {

  private JoinResultCollectors() {}

  public static org.apache.flink.util.Collector<JoinResult> emitting(
      org.apache.flink.util.Collector<JoinResult> downstream) {
    return new org.apache.flink.util.Collector<>() {
      @Override
      public void collect(JoinResult record) {
        FlinkOperatorTimings.recordJoinDelta();
        downstream.collect(record);
      }

      @Override
      public void close() {}
    };
  }

  public static org.apache.flink.util.Collector<JoinResult> metricsOnly() {
    return new org.apache.flink.util.Collector<>() {
      @Override
      public void collect(JoinResult record) {
        FlinkOperatorTimings.recordJoinDelta();
      }

      @Override
      public void close() {}
    };
  }
}
