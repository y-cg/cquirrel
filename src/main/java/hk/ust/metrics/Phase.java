package hk.ust.metrics;

/** Named execution phases for L0 run-level timing. */
public enum Phase {
  LOAD("load"),
  AJU("aju"),
  AGGREGATE("aggregate"),
  TOPK("topk"),
  SINK("sink"),
  /** Flink-only: per-record source collect and operator dispatch. */
  RUNTIME("runtime"),
  /** Flink-only: MiniCluster startup/teardown not attributed to operators. */
  CLUSTER("cluster");

  private final String id;

  Phase(String id) {
    this.id = id;
  }

  public String id() {
    return id;
  }
}
