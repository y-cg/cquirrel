package hk.ust.metrics;

/** Named execution phases for L0 run-level timing. */
public enum Phase {
  LOAD("load"),
  AJU("aju"),
  AGGREGATE("aggregate"),
  TOPK("topk"),
  SINK("sink");

  private final String id;

  Phase(String id) {
    this.id = id;
  }

  public String id() {
    return id;
  }
}
