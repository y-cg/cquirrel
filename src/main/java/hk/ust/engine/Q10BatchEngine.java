package hk.ust.engine;

import hk.ust.aju.JoinResult;
import hk.ust.aju.Q10ProcessFunction;
import hk.ust.aggregate.Q10RevenueAggregator;
import hk.ust.aggregate.Q10TopKWriter;
import hk.ust.metrics.Phase;
import hk.ust.model.TupleUpdate;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.flink.util.Collector;

/**
 * Shared Q10 maintenance core: AJU join + revenue aggregation with configurable input batching and
 * output materialization policy.
 */
public final class Q10BatchEngine {

  private final OutputPolicy outputPolicy;
  private final int inputBatchSize;
  private final Q10OutputSink sink;
  private final BiConsumer<Phase, Long> phaseTimer;
  private final Runnable joinDeltaHook;

  private Q10ProcessFunction aju;
  private Q10RevenueAggregator aggregator;
  private Q10TopKTracker topKTracker;
  private Collector<JoinResult> joinDeltaCollector;

  private long joinDeltasTotal;
  private long updatesInCurrentBatch;

  public Q10BatchEngine(OutputPolicy outputPolicy, int inputBatchSize, Q10OutputSink sink) {
    this(outputPolicy, inputBatchSize, sink, null, null);
  }

  public Q10BatchEngine(
      OutputPolicy outputPolicy,
      int inputBatchSize,
      Q10OutputSink sink,
      BiConsumer<Phase, Long> phaseTimer) {
    this(outputPolicy, inputBatchSize, sink, phaseTimer, null);
  }

  public Q10BatchEngine(
      OutputPolicy outputPolicy,
      int inputBatchSize,
      Q10OutputSink sink,
      BiConsumer<Phase, Long> phaseTimer,
      Runnable joinDeltaHook) {
    this.outputPolicy = outputPolicy;
    this.inputBatchSize = inputBatchSize <= 0 ? Integer.MAX_VALUE : inputBatchSize;
    this.sink = sink;
    this.phaseTimer = phaseTimer;
    this.joinDeltaHook = joinDeltaHook;
  }

  public void open() throws Exception {
    aju = new Q10ProcessFunction();
    aju.open(null);
    aggregator = new Q10RevenueAggregator();
    aggregator.open();
    if (usesIncrementalTopK()) {
      topKTracker = new Q10TopKTracker();
    }
    joinDeltaCollector =
        new Collector<>() {
          @Override
          public void collect(JoinResult record) {
            try {
              onJoinDelta(record);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void close() {}
        };
    joinDeltasTotal = 0;
    updatesInCurrentBatch = 0;
  }

  public void processUpdate(TupleUpdate update) throws Exception {
    long t0 = System.nanoTime();
    aju.processElement(update, null, joinDeltaCollector);
    recordPhase(Phase.AJU, t0);

    updatesInCurrentBatch++;
    if (updatesInCurrentBatch >= inputBatchSize) {
      endInputBatch();
    }
  }

  /** Marks the end of one input batch (table, micro-batch, or bounded source chunk). */
  public void endInputBatch() throws Exception {
    if (updatesInCurrentBatch == 0) {
      return;
    }
    if (outputPolicy == OutputPolicy.ON_BATCH_END) {
      emitTopK(topKTracker.snapshot(), OutputTrigger.BATCH_END);
    }
    updatesInCurrentBatch = 0;
  }

  /** Finalizes the job and emits according to {@link OutputPolicy#ON_JOB_END}. */
  public void finish() throws Exception {
    endInputBatch();

    if (outputPolicy == OutputPolicy.ON_JOB_END) {
      long t0 = System.nanoTime();
      List<Q10TopKRow> rows = Q10TopKWriter.buildTopKRows(aggregator);
      recordPhase(Phase.TOPK, t0);

      t0 = System.nanoTime();
      sink.emitTopK(rows, OutputTrigger.JOB_END);
      recordPhase(Phase.SINK, t0);

      System.out.printf("Total customer groups: %d%n", aggregator.revenueByCustomer().size());
    }
  }

  /** Finalizes pending input without materializing output; used by sharded runners. */
  public void finishWithoutOutput() throws Exception {
    endInputBatch();
  }

  public long joinDeltasTotal() {
    return joinDeltasTotal;
  }

  public OutputPolicy outputPolicy() {
    return outputPolicy;
  }

  public int inputBatchSize() {
    return inputBatchSize;
  }

  public Q10RevenueAggregator aggregator() {
    return aggregator;
  }

  private void onJoinDelta(JoinResult delta) throws Exception {
    long t0 = System.nanoTime();
    Q10RevenueAggregator.GroupState state = aggregator.applyAndGetState(delta);
    recordPhase(Phase.AGGREGATE, t0);
    joinDeltasTotal++;
    if (joinDeltaHook != null) {
      joinDeltaHook.run();
    }

    if (!usesIncrementalTopK()) {
      return;
    }

    boolean topKChanged =
        topKTracker.update(state.custkey(), state.revenueCents(), state.info());
    if (outputPolicy == OutputPolicy.ON_EACH_DELTA && topKChanged) {
      emitTopK(topKTracker.snapshot(), OutputTrigger.DELTA);
    }
  }

  private void emitTopK(List<Q10TopKRow> rows, OutputTrigger trigger) throws Exception {
    sink.emitTopK(rows, trigger);
  }

  private boolean usesIncrementalTopK() {
    return outputPolicy == OutputPolicy.ON_EACH_DELTA || outputPolicy == OutputPolicy.ON_BATCH_END;
  }

  private void recordPhase(Phase phase, long startNs) {
    if (phaseTimer != null) {
      phaseTimer.accept(phase, System.nanoTime() - startNs);
    }
  }
}
