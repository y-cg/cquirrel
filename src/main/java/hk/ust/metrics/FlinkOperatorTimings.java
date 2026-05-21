package hk.ust.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe phase accumulators for Flink task threads. Merged into {@link RunMetrics} on the
 * driver thread after {@code env.execute()} returns.
 */
public final class FlinkOperatorTimings {

  private static final LongAdder[] PHASE_NS = new LongAdder[Phase.values().length];
  private static final LongAdder JOIN_DELTAS = new LongAdder();

  static {
    for (int i = 0; i < PHASE_NS.length; i++) {
      PHASE_NS[i] = new LongAdder();
    }
  }

  private FlinkOperatorTimings() {}

  public static void reset() {
    for (LongAdder adder : PHASE_NS) {
      adder.reset();
    }
    JOIN_DELTAS.reset();
  }

  public static void add(Phase phase, long durationNs) {
    if (durationNs > 0) {
      PHASE_NS[phase.ordinal()].add(durationNs);
    }
  }

  public static void recordJoinDelta() {
    JOIN_DELTAS.increment();
  }

  public static long phaseNs(Phase phase) {
    return PHASE_NS[phase.ordinal()].sum();
  }

  public static long joinDeltasTotal() {
    return JOIN_DELTAS.sum();
  }

  public static long operatorTotalNs() {
    long total = 0;
    for (Phase phase : Phase.values()) {
      if (phase == Phase.CLUSTER) {
        continue;
      }
      total += phaseNs(phase);
    }
    return total;
  }

  public static void flushTo(RunMetrics metrics) {
    for (Phase phase : Phase.values()) {
      if (phase == Phase.CLUSTER) {
        continue;
      }
      long ns = phaseNs(phase);
      if (ns > 0) {
        metrics.addPhaseDuration(phase, ns);
      }
    }
    metrics.setJoinDeltasTotal(joinDeltasTotal());
  }

  public static void recordClusterOverhead(long executeWallNs) {
    long gap = executeWallNs - operatorTotalNs();
    if (gap > 0) {
      add(Phase.CLUSTER, gap);
    }
  }

  public static void flushClusterTo(RunMetrics metrics) {
    long ns = phaseNs(Phase.CLUSTER);
    if (ns > 0) {
      metrics.addPhaseDuration(Phase.CLUSTER, ns);
    }
  }
}
