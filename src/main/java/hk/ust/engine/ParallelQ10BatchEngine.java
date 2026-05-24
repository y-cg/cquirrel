package hk.ust.engine;

import hk.ust.aggregate.Q10AggregateMerger;
import hk.ust.aggregate.Q10RevenueAggregator;
import hk.ust.aggregate.Q10TopKWriter;
import hk.ust.metrics.Phase;
import hk.ust.metrics.PhaseTimingAccumulator;
import hk.ust.model.TupleUpdate;
import hk.ust.model.TupleUpdate.CustomerUpdate;
import hk.ust.model.TupleUpdate.LineitemUpdate;
import hk.ust.model.TupleUpdate.NationUpdate;
import hk.ust.model.TupleUpdate.OrdersUpdate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Multi-core standalone Q10 engine.
 *
 * <p>Each shard owns an independent {@link Q10BatchEngine}. Dimension tuples are broadcast to all
 * shards, while orders and lineitems are partitioned by order key so one order chain is maintained
 * by exactly one worker.
 */
public final class ParallelQ10BatchEngine implements AutoCloseable {

  private static final int WORKER_INPUT_BUFFER_SIZE = 4096;

  private final int parallelism;
  private final OutputPolicy outputPolicy;
  private final int inputBatchSize;
  private final Q10OutputSink sink;
  private final PhaseTimingAccumulator timings;
  private final List<ShardWorker> workers;

  private long updatesInCurrentBatch;
  private long joinDeltasTotal;
  private boolean finished;

  public ParallelQ10BatchEngine(
      int parallelism,
      OutputPolicy outputPolicy,
      int inputBatchSize,
      Q10OutputSink sink,
      PhaseTimingAccumulator timings) {
    if (outputPolicy == OutputPolicy.ON_EACH_DELTA) {
      throw new IllegalArgumentException(
          "Parallel standalone benchmark supports ON_JOB_END and ON_BATCH_END only");
    }
    this.parallelism = Math.max(1, parallelism);
    this.outputPolicy = outputPolicy;
    this.inputBatchSize = inputBatchSize <= 0 ? Integer.MAX_VALUE : inputBatchSize;
    this.sink = sink;
    this.timings = timings;
    this.workers = new ArrayList<>(this.parallelism);
    for (int i = 0; i < this.parallelism; i++) {
      workers.add(new ShardWorker(i, timings));
    }
  }

  public void open() throws Exception {
    for (ShardWorker worker : workers) {
      worker.open();
    }
  }

  public void processUpdate(TupleUpdate update) throws Exception {
    switch (update) {
      case NationUpdate ignored -> broadcast(update);
      case CustomerUpdate ignored -> broadcast(update);
      case OrdersUpdate u -> workerForOrderKey(u.tuple().oOrderkey()).enqueue(update);
      case LineitemUpdate u -> workerForOrderKey(u.tuple().lOrderkey()).enqueue(update);
    }

    updatesInCurrentBatch++;
    if (updatesInCurrentBatch >= inputBatchSize) {
      endInputBatch();
    }
  }

  /** Marks a global input batch boundary and emits one merged snapshot for ON_BATCH_END. */
  public void endInputBatch() throws Exception {
    if (updatesInCurrentBatch == 0) {
      return;
    }
    awaitWorkers();
    if (outputPolicy == OutputPolicy.ON_BATCH_END) {
      emitMergedTopK(OutputTrigger.BATCH_END);
    }
    updatesInCurrentBatch = 0;
  }

  public void finish() throws Exception {
    if (finished) {
      return;
    }
    try {
      endInputBatch();
      for (ShardWorker worker : workers) {
        worker.finish();
      }
      joinDeltasTotal = workers.stream().mapToLong(ShardWorker::joinDeltasTotal).sum();
      if (outputPolicy == OutputPolicy.ON_JOB_END) {
        emitMergedTopK(OutputTrigger.JOB_END);
      }
      finished = true;
    } finally {
      close();
    }
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

  public int parallelism() {
    return parallelism;
  }

  @Override
  public void close() {
    for (ShardWorker worker : workers) {
      worker.close();
    }
  }

  private void broadcast(TupleUpdate update) throws Exception {
    for (ShardWorker worker : workers) {
      worker.enqueue(update);
    }
  }

  private ShardWorker workerForOrderKey(long orderKey) {
    int index = Math.floorMod(Long.hashCode(orderKey), parallelism);
    return workers.get(index);
  }

  private void awaitWorkers() throws Exception {
    for (ShardWorker worker : workers) {
      worker.await();
    }
  }

  private void emitMergedTopK(OutputTrigger trigger) throws Exception {
    long t0 = System.nanoTime();
    Q10RevenueAggregator merged = Q10AggregateMerger.merge(workerAggregates());
    timings.add(Phase.AGGREGATE, System.nanoTime() - t0);

    t0 = System.nanoTime();
    List<Q10TopKRow> rows = Q10TopKWriter.buildTopKRows(merged);
    timings.add(Phase.TOPK, System.nanoTime() - t0);

    t0 = System.nanoTime();
    sink.emitTopK(rows, trigger);
    timings.add(Phase.SINK, System.nanoTime() - t0);

    if (trigger == OutputTrigger.JOB_END) {
      System.out.printf("Total customer groups: %d%n", merged.revenueByCustomer().size());
    }
  }

  private Collection<Q10RevenueAggregator> workerAggregates() {
    return workers.stream().map(ShardWorker::aggregator).toList();
  }

  private static final class ShardWorker implements AutoCloseable {

    private final int shardId;
    private final ExecutorService executor;
    private final List<TupleUpdate> buffer = new ArrayList<>(WORKER_INPUT_BUFFER_SIZE);
    private final List<Future<?>> futures = new ArrayList<>();
    private final Q10BatchEngine engine;

    ShardWorker(int shardId, PhaseTimingAccumulator timings) {
      this.shardId = shardId;
      this.executor =
          Executors.newSingleThreadExecutor(
              task -> {
                Thread thread = new Thread(task, "q10-shard-" + shardId);
                thread.setDaemon(true);
                return thread;
              });
      this.engine =
          new Q10BatchEngine(
              OutputPolicy.ON_JOB_END,
              Integer.MAX_VALUE,
              new NoopQ10OutputSink(),
              timings::add);
    }

    void open() throws Exception {
      Future<?> future = executor.submit(this::openEngine);
      waitFor(future);
    }

    void enqueue(TupleUpdate update) throws Exception {
      buffer.add(update);
      if (buffer.size() >= WORKER_INPUT_BUFFER_SIZE) {
        flush();
      }
    }

    void await() throws Exception {
      flush();
      for (Future<?> future : futures) {
        waitFor(future);
      }
      futures.clear();
    }

    void finish() throws Exception {
      await();
      Future<?> future = executor.submit(this::finishEngine);
      waitFor(future);
    }

    long joinDeltasTotal() {
      return engine.joinDeltasTotal();
    }

    Q10RevenueAggregator aggregator() {
      return engine.aggregator();
    }

    @Override
    public void close() {
      executor.shutdownNow();
    }

    private void flush() {
      if (buffer.isEmpty()) {
        return;
      }
      List<TupleUpdate> batch = List.copyOf(buffer);
      buffer.clear();
      futures.add(executor.submit(() -> processBatch(batch)));
    }

    private void openEngine() {
      try {
        engine.open();
      } catch (Exception e) {
        throw new RuntimeException("Failed to open shard " + shardId, e);
      }
    }

    private void processBatch(List<TupleUpdate> batch) {
      for (TupleUpdate update : batch) {
        try {
          engine.processUpdate(update);
        } catch (Exception e) {
          throw new RuntimeException("Failed to process update on shard " + shardId, e);
        }
      }
    }

    private void finishEngine() {
      try {
        engine.finishWithoutOutput();
      } catch (Exception e) {
        throw new RuntimeException("Failed to finish shard " + shardId, e);
      }
    }

    private static void waitFor(Future<?> future) throws Exception {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException runtimeException) {
          throw runtimeException;
        }
        if (cause instanceof Exception exception) {
          throw exception;
        }
        throw new RuntimeException(cause);
      }
    }
  }
}
