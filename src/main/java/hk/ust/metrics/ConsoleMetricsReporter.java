package hk.ust.metrics;

/** Human-readable L0 metrics summary on stdout. */
public final class ConsoleMetricsReporter implements MetricsReporter {

  @Override
  public void report(MetricsSnapshot snapshot) {
    System.out.println();
    System.out.println("=== CQuirrel L0 Metrics ===");
    System.out.printf("run_id:              %s%n", snapshot.runId());
    System.out.printf("runner:              %s%n", snapshot.runner());
    System.out.printf("git_commit:          %s%n", snapshot.gitCommit());
    System.out.printf("updates_total:       %,d%n", snapshot.updatesTotal());
    System.out.printf("join_deltas_total:   %,d%n", snapshot.joinDeltasTotal());
    System.out.println("phases_ms:");
    for (var entry : snapshot.phasesMs().entrySet()) {
      System.out.printf("  %-12s %,d%n", entry.getKey() + ":", entry.getValue());
    }
    System.out.printf("wall_time_ms:        %,d%n", snapshot.wallTimeMs());
    System.out.printf("processing_time_ms:  %,d%n", snapshot.processingTimeMs());
    System.out.printf("updates_per_sec:     %.2f%n", snapshot.updatesPerSec());
    System.out.printf("join_deltas_per_sec: %.2f%n", snapshot.joinDeltasPerSec());
    System.out.printf("heap_used_mb_peak:   %,d%n", snapshot.heapUsedMbPeak());
    System.out.printf("heap_max_mb:         %,d%n", snapshot.heapMaxMb());
    System.out.printf("preload_updates:     %,d%n", snapshot.preloadUpdatesCount());
    System.out.println("===========================");
  }
}
