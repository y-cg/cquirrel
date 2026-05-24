package hk.ust.metrics;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * L0 run-level metrics: phase timings, throughput, and peak heap usage.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (RunMetrics metrics = RunMetrics.start("standalone", tpchDataDir)) {
 *   try (var phase = metrics.phase(Phase.LOAD)) { ... }
 *   metrics.setUpdatesTotal(n);
 *   metrics.setJoinDeltasTotal(m);
 * }
 * }</pre>
 */
public class RunMetrics implements AutoCloseable {

  private static final RunMetrics DISABLED = new DisabledRunMetrics();

  private final String runId;
  private final String runner;
  private final String tpchDataDir;
  private final long wallStartNs;
  private final MemorySampler memorySampler = new MemorySampler();
  private final Map<Phase, Long> phaseDurationsNs = new EnumMap<>(Phase.class);
  private final List<MetricsReporter> reporters;

  private long updatesTotal;
  private long joinDeltasTotal;
  private long preloadUpdatesCount;
  private int parallelism = 1;

  private RunMetrics(String runner, String tpchDataDir, List<MetricsReporter> reporters) {
    this.runId = MetricsSnapshot.newRunId();
    this.runner = runner;
    this.tpchDataDir = tpchDataDir;
    this.wallStartNs = System.nanoTime();
    this.reporters = reporters;
    for (Phase phase : Phase.values()) {
      phaseDurationsNs.put(phase, 0L);
    }
    memorySampler.sample();
  }

  public static RunMetrics start(String runner, String tpchDataDir) {
    if (!MetricsConfig.enabled()) {
      return DISABLED;
    }
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new ConsoleMetricsReporter());
    String outputPath = MetricsConfig.metricsOutputPath();
    if (outputPath != null) {
      reporters.add(new JsonLineMetricsReporter(java.nio.file.Path.of(outputPath)));
    }
    return new RunMetrics(runner, tpchDataDir, reporters);
  }

  /** Starts timing a phase; closes automatically when the returned scope is closed. */
  public PhaseScope phase(Phase phase) {
    if (this == DISABLED) {
      return PhaseScope.noop();
    }
    return new PhaseScope(this, phase);
  }

  void endPhase(Phase phase, long durationNs) {
    addPhaseDuration(phase, durationNs);
  }

  /** Adds nanoseconds to a phase total (for fine-grained loops without per-iteration scopes). */
  public void addPhaseDuration(Phase phase, long durationNs) {
    if (this == DISABLED) {
      return;
    }
    phaseDurationsNs.merge(phase, durationNs, Long::sum);
    memorySampler.sample();
  }

  public void sampleMemory() {
    if (this != DISABLED) {
      memorySampler.sample();
    }
  }

  public void setUpdatesTotal(long updatesTotal) {
    this.updatesTotal = updatesTotal;
  }

  public void setJoinDeltasTotal(long joinDeltasTotal) {
    this.joinDeltasTotal = joinDeltasTotal;
  }

  public void setPreloadUpdatesCount(long preloadUpdatesCount) {
    this.preloadUpdatesCount = preloadUpdatesCount;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = Math.max(1, parallelism);
  }

  @Override
  public void close() {
    if (this == DISABLED) {
      return;
    }
    memorySampler.sample();
    MetricsSnapshot snapshot = buildSnapshot();
    for (MetricsReporter reporter : reporters) {
      reporter.report(snapshot);
    }
  }

  /** Returns phase durations accumulated so far (milliseconds). */
  public Map<String, Long> phasesMsSnapshot() {
    Map<String, Long> phasesMs = new LinkedHashMap<>();
    for (Phase phase : Phase.values()) {
      phasesMs.put(phase.id(), nanosToMillis(phaseDurationsNs.get(phase)));
    }
    return phasesMs;
  }

  private MetricsSnapshot buildSnapshot() {
    long wallTimeMs = nanosToMillis(System.nanoTime() - wallStartNs);

    Map<String, Long> phasesMs = new LinkedHashMap<>();
    long processingTimeMs = 0;
    for (Phase phase : Phase.values()) {
      long phaseMs = nanosToMillis(phaseDurationsNs.get(phase));
      phasesMs.put(phase.id(), phaseMs);
      if (phase != Phase.LOAD && phase != Phase.RUNTIME && phase != Phase.CLUSTER) {
        processingTimeMs += phaseMs;
      }
    }

    long throughputTimeMs = parallelism > 1 ? wallTimeMs : processingTimeMs;
    double throughputSec = throughputTimeMs / 1000.0;
    double updatesPerSec = throughputSec > 0 ? updatesTotal / throughputSec : 0;
    double joinDeltasPerSec = throughputSec > 0 ? joinDeltasTotal / throughputSec : 0;

    return new MetricsSnapshot(
        runId,
        runner,
        tpchDataDir,
        resolveGitCommit(),
        parallelism,
        updatesTotal,
        joinDeltasTotal,
        Map.copyOf(phasesMs),
        wallTimeMs,
        processingTimeMs,
        updatesPerSec,
        joinDeltasPerSec,
        MemorySampler.toMb(memorySampler.peakUsedBytes()),
        MemorySampler.toMb(memorySampler.maxBytes()),
        preloadUpdatesCount);
  }

  private static long nanosToMillis(long nanos) {
    return nanos / 1_000_000L;
  }

  private static String resolveGitCommit() {
    try {
      Process process =
          new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
              .redirectErrorStream(true)
              .start();
      try (var reader =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line = reader.readLine();
        if (process.waitFor() == 0 && line != null && !line.isBlank()) {
          return line.trim();
        }
      }
    } catch (Exception ignored) {
      // Best-effort only; metrics should not fail the run.
    }
    return "unknown";
  }

  /** Auto-closeable scope for a single phase timer. */
  public static class PhaseScope implements AutoCloseable {

    private final RunMetrics metrics;
    private final Phase phase;
    private final long startNs;
    private boolean closed;

    PhaseScope(RunMetrics metrics, Phase phase) {
      this.metrics = metrics;
      this.phase = phase;
      this.startNs = System.nanoTime();
    }

    private static PhaseScope noop() {
      return new NoopPhaseScope();
    }

    @Override
    public void close() {
      if (closed || metrics == null) {
        return;
      }
      closed = true;
      metrics.endPhase(phase, System.nanoTime() - startNs);
    }
  }

  private static final class NoopPhaseScope extends PhaseScope {

    private NoopPhaseScope() {
      super(null, null);
    }

    @Override
    public void close() {}
  }

  private static final class DisabledRunMetrics extends RunMetrics {

    private DisabledRunMetrics() {
      super("disabled", "", List.of());
    }

    @Override
    public PhaseScope phase(Phase phase) {
      return PhaseScope.noop();
    }

    @Override
    public void setUpdatesTotal(long updatesTotal) {}

    @Override
    public void setJoinDeltasTotal(long joinDeltasTotal) {}

    @Override
    public void setPreloadUpdatesCount(long preloadUpdatesCount) {}

    @Override
    public void setParallelism(int parallelism) {}

    @Override
    public void close() {}
  }
}
