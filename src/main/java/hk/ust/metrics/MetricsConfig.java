package hk.ust.metrics;

/** Environment-driven configuration for L0 run metrics. */
public final class MetricsConfig {

  private MetricsConfig() {}

  public static boolean enabled() {
    String value = System.getenv("CQUIRREL_METRICS");
    if (value == null || value.isBlank()) {
      return true;
    }
    return !value.equalsIgnoreCase("off") && !value.equalsIgnoreCase("false");
  }

  public static String metricsOutputPath() {
    String path = System.getenv("CQUIRREL_METRICS_OUT");
    return path == null || path.isBlank() ? null : path;
  }
}
