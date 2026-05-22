package hk.ust.engine;

/** Reason a top-K snapshot was emitted. */
public enum OutputTrigger {
  DELTA,
  BATCH_END,
  JOB_END
}
