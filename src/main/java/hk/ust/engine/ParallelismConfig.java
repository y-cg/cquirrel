package hk.ust.engine;

/** Resolves Flink pipeline mode and operator parallelism from env / system properties. */
public final class ParallelismConfig {

  public static final String ENV_PARALLELISM = "CQUIRREL_PARALLELISM";
  public static final String PROP_PARALLELISM = "q10.parallelism";
  public static final String ENV_AJU_PARALLELISM = "CQUIRREL_AJU_PARALLELISM";
  public static final String ENV_AGG_PARALLELISM = "CQUIRREL_AGG_PARALLELISM";
  public static final String ENV_PIPELINE = "CQUIRREL_PIPELINE";
  public static final String PROP_PIPELINE = "q10.pipeline";

  public enum PipelineMode {
    UNIFIED,
    SPLIT,
    PARTITIONED
  }

  private ParallelismConfig() {}

  public static int global() {
    return positiveInt(envOrProp(ENV_PARALLELISM, PROP_PARALLELISM, "1"), 1);
  }

  public static int ajuParallelism() {
    String value = System.getenv(ENV_AJU_PARALLELISM);
    if (value == null || value.isBlank()) {
      return global();
    }
    return positiveInt(value, global());
  }

  public static int aggParallelism() {
    String value = System.getenv(ENV_AGG_PARALLELISM);
    if (value == null || value.isBlank()) {
      return global();
    }
    return positiveInt(value, global());
  }

  public static PipelineMode pipelineMode() {
    String value = System.getenv(ENV_PIPELINE);
    if (value == null || value.isBlank()) {
      value = System.getProperty(PROP_PIPELINE, PipelineMode.UNIFIED.name());
    }
    return PipelineMode.valueOf(value.trim().toUpperCase());
  }

  public static boolean isPartitionedAju() {
    return pipelineMode() == PipelineMode.PARTITIONED;
  }

  private static String envOrProp(String envKey, String propKey, String defaultValue) {
    String value = System.getenv(envKey);
    if (value == null || value.isBlank()) {
      value = System.getProperty(propKey, defaultValue);
    }
    return value;
  }

  private static int positiveInt(String value, int fallback) {
    try {
      int parsed = Integer.parseInt(value.trim());
      return parsed > 0 ? parsed : fallback;
    } catch (NumberFormatException e) {
      return fallback;
    }
  }
}
