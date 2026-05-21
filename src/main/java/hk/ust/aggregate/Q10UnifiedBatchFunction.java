package hk.ust.aggregate;

import hk.ust.aju.JoinResult;
import hk.ust.aju.Q10ProcessFunction;
import hk.ust.metrics.FlinkOperatorTimings;
import hk.ust.metrics.Phase;
import hk.ust.model.TupleUpdate;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Single-operator Flink path aligned with {@link hk.ust.StandaloneRunner}:
 *
 * <p>AJU join maintenance + in-operator revenue aggregation (no per-delta downstream records) + one
 * top-K sort and CSV write in {@link #close()}.
 */
public class Q10UnifiedBatchFunction
    extends ProcessFunction<TupleUpdate, Q10Aggregator.AggregateResult> {

  private static final String DEFAULT_OUTPUT = "result/flink-q10.csv";

  private transient Q10ProcessFunction aju;
  private transient Q10RevenueAggregator aggregator;
  private transient Collector<JoinResult> joinDeltaCollector;

  private final String outputPath;

  public Q10UnifiedBatchFunction() {
    this(System.getProperty("q10.output", DEFAULT_OUTPUT));
  }

  public Q10UnifiedBatchFunction(String outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    aju = new Q10ProcessFunction();
    aju.open(openContext);
    aggregator = new Q10RevenueAggregator();
    aggregator.open();
    joinDeltaCollector =
        new Collector<JoinResult>() {
          @Override
          public void collect(JoinResult record) {
            long t0 = System.nanoTime();
            aggregator.apply(record);
            FlinkOperatorTimings.add(Phase.AGGREGATE, System.nanoTime() - t0);
            FlinkOperatorTimings.recordJoinDelta();
          }

          @Override
          public void close() {}
        };
  }

  @Override
  public void processElement(
      TupleUpdate update, Context ctx, Collector<Q10Aggregator.AggregateResult> out) {
    long t0 = System.nanoTime();
    aju.processElement(update, null, joinDeltaCollector);
    FlinkOperatorTimings.add(Phase.AJU, System.nanoTime() - t0);
  }

  @Override
  public void close() throws Exception {
    try {
      Q10TopKWriter.writeCsvTimed(aggregator, outputPath, FlinkOperatorTimings::add);
    } finally {
      super.close();
    }
  }
}
