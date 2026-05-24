package hk.ust.metrics;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** Thread-safe phase timing accumulator for multi-worker benchmark runs. */
public final class PhaseTimingAccumulator {

  private final Map<Phase, LongAdder> phaseNs = new EnumMap<>(Phase.class);

  public PhaseTimingAccumulator() {
    for (Phase phase : Phase.values()) {
      phaseNs.put(phase, new LongAdder());
    }
  }

  public void add(Phase phase, long durationNs) {
    if (durationNs > 0) {
      phaseNs.get(phase).add(durationNs);
    }
  }

  public void flushTo(RunMetrics metrics) {
    for (Phase phase : Phase.values()) {
      long ns = phaseNs.get(phase).sum();
      if (ns > 0) {
        metrics.addPhaseDuration(phase, ns);
      }
    }
  }
}
