package hk.ust.metrics;

/** Writes a completed L0 metrics snapshot. */
public interface MetricsReporter {

  void report(MetricsSnapshot snapshot);
}
