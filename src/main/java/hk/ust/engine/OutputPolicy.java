package hk.ust.engine;

/**
 * Controls when materialized Q10 top-K results are emitted to the {@link Q10OutputSink}.
 *
 * <p>Input batching ({@link Q10BatchEngine#endInputBatch()}) is independent of this policy.
 */
public enum OutputPolicy {
  /** Emit whenever the top-20 set changes after a join delta (finest incremental output). */
  ON_EACH_DELTA,
  /** Emit the current top-20 after each input batch boundary. */
  ON_BATCH_END,
  /** Emit only once when {@link Q10BatchEngine#finish()} is called (bulk snapshot). */
  ON_JOB_END
}
