package hk.ust.engine;

/** Resolves {@link OutputPolicy} and input batch size from env / system properties. */
public final class OutputPolicyConfig {

  public static final String ENV_POLICY = "CQUIRREL_OUTPUT_POLICY";
  public static final String PROP_POLICY = "q10.output.policy";
  public static final String ENV_INPUT_BATCH_SIZE = "CQUIRREL_INPUT_BATCH_SIZE";
  public static final String PROP_INPUT_BATCH_SIZE = "q10.input.batch.size";

  private OutputPolicyConfig() {}

  public static OutputPolicy outputPolicy() {
    String value = System.getenv(ENV_POLICY);
    if (value == null || value.isBlank()) {
      value = System.getProperty(PROP_POLICY, OutputPolicy.ON_JOB_END.name());
    }
    return OutputPolicy.valueOf(value.trim().toUpperCase());
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
}
