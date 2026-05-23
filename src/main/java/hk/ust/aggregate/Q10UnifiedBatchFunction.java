package hk.ust.aggregate;

import hk.ust.engine.CsvQ10OutputSink;
import hk.ust.engine.OutputPolicyConfig;
import hk.ust.engine.Q10BatchEngine;
import hk.ust.metrics.FlinkOperatorTimings;
import hk.ust.model.TupleUpdate;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink operator wrapper around {@link Q10BatchEngine}.
 */
public class Q10UnifiedBatchFunction
    extends ProcessFunction<TupleUpdate, Q10Aggregator.AggregateResult> {

  private static final String DEFAULT_OUTPUT = "result/flink-q10.csv";

  private static volatile long lastJoinDeltasTotal;

  private transient Q10BatchEngine engine;
  private final String outputPath;

  public Q10UnifiedBatchFunction() {
    this(System.getProperty("q10.output", DEFAULT_OUTPUT));
  }

  public Q10UnifiedBatchFunction(String outputPath) {
    this.outputPath = outputPath;
  }

  public static long lastJoinDeltasTotal() {
    return lastJoinDeltasTotal;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    engine =
        new Q10BatchEngine(
            OutputPolicyConfig.outputPolicy(),
            OutputPolicyConfig.inputBatchSize(),
            new CsvQ10OutputSink(outputPath),
            FlinkOperatorTimings::add,
            FlinkOperatorTimings::recordJoinDelta);
    engine.open();
    System.out.printf(
        "Q10BatchEngine policy=%s inputBatchSize=%s%n",
        engine.outputPolicy(),
        engine.inputBatchSize() == Integer.MAX_VALUE ? "unbounded" : engine.inputBatchSize());
  }

  @Override
  public void processElement(
      TupleUpdate update, Context ctx, Collector<Q10Aggregator.AggregateResult> out) {
    try {
      engine.processUpdate(update);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    try {
      engine.finish();
      lastJoinDeltasTotal = engine.joinDeltasTotal();
    } finally {
      super.close();
    }
  }
}
