package hk.ust.engine;

import java.util.Locale;

/** Resolves {@link OutputPolicy} and input batch size from env / system properties. */
public final class OutputPolicyConfig {

  public static final String ENV_POLICY = "CQUIRREL_OUTPUT_POLICY";
  public static final String PROP_POLICY = "q10.output.policy";
  public static final String ENV_INPUT_BATCH_SIZE = "CQUIRREL_INPUT_BATCH_SIZE";
  public static final String PROP_INPUT_BATCH_SIZE = "q10.input.batch.size";
  public static final String ENV_PARALLELISM = "CQUIRREL_PARALLELISM";
  public static final String PROP_PARALLELISM = "q10.parallelism";

  private OutputPolicyConfig() {}

  public static OutputPolicy outputPolicy() {
    String value = System.getenv(ENV_POLICY);
    if (value == null || value.isBlank()) {
      value = System.getProperty(PROP_POLICY, OutputPolicy.ON_JOB_END.name());
    }
    return OutputPolicy.valueOf(value.trim().toUpperCase(Locale.ROOT));
  }

  /** Number of tuple updates per input batch; {@code 1} is the finest micro-batch. */
  public static int inputBatchSize() {
    String value = System.getenv(ENV_INPUT_BATCH_SIZE);
    if (value == null || value.isBlank()) {
      value = System.getProperty(PROP_INPUT_BATCH_SIZE, "0");
    }
    int size = Integer.parseInt(value.trim());
    return size <= 0 ? Integer.MAX_VALUE : size;
  }

  /** Number of independent Q10 engine shards for multi-core standalone benchmark runs. */
  public static int parallelism() {
    String value = System.getenv(ENV_PARALLELISM);
    if (value == null || value.isBlank()) {
      value = System.getProperty(PROP_PARALLELISM, "1");
    }
    int parallelism = Integer.parseInt(value.trim());
    return Math.max(1, parallelism);
  }
}
