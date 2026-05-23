package hk.ust.aggregate;

import hk.ust.aju.JoinResult;
import hk.ust.metrics.FlinkOperatorTimings;
import hk.ust.metrics.Phase;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Wraps {@link Q10Aggregator} with aggregate-phase timing. */
public class TimedQ10Aggregator
    extends ProcessFunction<JoinResult, Q10Aggregator.AggregateResult> {

  private transient Q10Aggregator delegate;

  @Override
  public void open(OpenContext openContext) {
    delegate = new Q10Aggregator();
    delegate.open(openContext);
  }

  @Override
  public void processElement(
      JoinResult jr, Context ctx, Collector<Q10Aggregator.AggregateResult> out) {
    long t0 = System.nanoTime();
    delegate.processElement(jr, ctx, out);
    FlinkOperatorTimings.add(Phase.AGGREGATE, System.nanoTime() - t0);
  }
}
