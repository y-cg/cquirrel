package hk.ust.sink;

import hk.ust.aggregate.Q10Aggregator.AggregateResult;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

/**
 * Simple sink that prints Q10 aggregate results to stdout. In production this would write to an
 * external system.
 */
public class DeltaSink implements SinkFunction<AggregateResult> {

  @Override
  public void invoke(AggregateResult value, Context context) {
    // Intentionally no-op: suppress output during benchmarking to avoid stdout I/O bottleneck.
  }
}
