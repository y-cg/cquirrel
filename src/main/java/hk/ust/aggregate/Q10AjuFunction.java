package hk.ust.aggregate;

import hk.ust.aju.JoinResult;
import hk.ust.aju.Q10ProcessFunction;
import hk.ust.metrics.FlinkOperatorTimings;
import hk.ust.metrics.Phase;
import hk.ust.model.TupleUpdate;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Flink operator that runs AJU join maintenance and emits {@link JoinResult} deltas downstream. */
public class Q10AjuFunction extends ProcessFunction<TupleUpdate, JoinResult> {

  private transient Q10ProcessFunction aju;

  @Override
  public void open(OpenContext openContext) {
    aju = new Q10ProcessFunction();
    aju.open(openContext);
  }

  @Override
  public void processElement(TupleUpdate update, Context ctx, Collector<JoinResult> out) {
    long t0 = System.nanoTime();
    aju.processElement(update, ctx, JoinResultCollectors.emitting(out));
    FlinkOperatorTimings.add(Phase.AJU, System.nanoTime() - t0);
  }
}
