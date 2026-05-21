package hk.ust.metrics;

import java.util.Map;
import java.util.UUID;

/** Immutable L0 metrics for a single program run. */
public record MetricsSnapshot(
    String runId,
    String runner,
    String tpchDataDir,
    String gitCommit,
    long updatesTotal,
    long joinDeltasTotal,
    Map<String, Long> phasesMs,
    long wallTimeMs,
    long processingTimeMs,
    double updatesPerSec,
    double joinDeltasPerSec,
    long heapUsedMbPeak,
    long heapMaxMb,
    long preloadUpdatesCount) {

  public String toJsonLine() {
    StringBuilder sb = new StringBuilder(256);
    sb.append('{');
    appendJsonField(sb, "run_id", runId, true);
    appendJsonField(sb, "runner", runner, false);
    appendJsonField(sb, "tpch_data_dir", tpchDataDir, false);
    appendJsonField(sb, "git_commit", gitCommit, false);
    sb.append("\"updates_total\":").append(updatesTotal).append(',');
    sb.append("\"join_deltas_total\":").append(joinDeltasTotal).append(',');
    sb.append("\"phases_ms\":").append(phasesMsToJson()).append(',');
    sb.append("\"wall_time_ms\":").append(wallTimeMs).append(',');
    sb.append("\"processing_time_ms\":").append(processingTimeMs).append(',');
    sb.append("\"throughput\":{");
    sb.append("\"updates_per_sec\":").append(formatDouble(updatesPerSec)).append(',');
    sb.append("\"join_deltas_per_sec\":").append(formatDouble(joinDeltasPerSec));
    sb.append("},");
    sb.append("\"memory\":{");
    sb.append("\"heap_used_mb_peak\":").append(heapUsedMbPeak).append(',');
    sb.append("\"heap_max_mb\":").append(heapMaxMb).append(',');
    sb.append("\"preload_updates_count\":").append(preloadUpdatesCount);
    sb.append("}}");
    return sb.toString();
  }

  private String phasesMsToJson() {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    boolean first = true;
    for (Map.Entry<String, Long> entry : phasesMs.entrySet()) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      sb.append('"').append(escapeJson(entry.getKey())).append("\":").append(entry.getValue());
    }
    sb.append('}');
    return sb.toString();
  }

  private static void appendJsonField(StringBuilder sb, String key, String value, boolean first) {
    if (!first) {
      sb.append(',');
    }
    sb.append('"').append(key).append("\":\"").append(escapeJson(value)).append('"');
  }

  private static String escapeJson(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String formatDouble(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      return "0";
    }
    return String.format("%.2f", value);
  }

  static String newRunId() {
    return UUID.randomUUID().toString();
  }
}
