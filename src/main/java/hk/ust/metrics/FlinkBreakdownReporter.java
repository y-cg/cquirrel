package hk.ust.metrics;

import java.util.Map;

/** Prints Flink execute timing split to align with standalone phase names. */
public final class FlinkBreakdownReporter {

  private FlinkBreakdownReporter() {}

  public static void print(long executeWallMs, Map<String, Long> phasesMs) {
    long load = phasesMs.getOrDefault(Phase.LOAD.id(), 0L);
    long runtime = phasesMs.getOrDefault(Phase.RUNTIME.id(), 0L);
    long aju = phasesMs.getOrDefault(Phase.AJU.id(), 0L);
    long aggregate = phasesMs.getOrDefault(Phase.AGGREGATE.id(), 0L);
    long topk = phasesMs.getOrDefault(Phase.TOPK.id(), 0L);
    long sink = phasesMs.getOrDefault(Phase.SINK.id(), 0L);
    long cluster = phasesMs.getOrDefault(Phase.CLUSTER.id(), 0L);

    long compute = aju + aggregate + topk + sink;
    long flinkTax = runtime + cluster;
    long accounted = load + flinkTax + compute;

    System.out.println();
    System.out.println("=== Flink execute breakdown (standalone-aligned phases) ===");
    System.out.printf("execute_wall_ms:       %,d%n", executeWallMs);
    System.out.println("phases_ms:");
    System.out.printf(
        "  %-12s %,d   (read + parse; same role as standalone LOAD)%n", Phase.LOAD.id() + ":", load);
    System.out.printf(
        "  %-12s %,d   (Flink-only: Source.collect + operator dispatch)%n",
        Phase.RUNTIME.id() + ":", runtime);
    System.out.printf(
        "  %-12s %,d   (Q10ProcessFunction.processElement)%n", Phase.AJU.id() + ":", aju);
    System.out.printf(
        "  %-12s %,d   (inline revenue merge per join delta)%n",
        Phase.AGGREGATE.id() + ":", aggregate);
    System.out.printf("  %-12s %,d   (final sort)%n", Phase.TOPK.id() + ":", topk);
    System.out.printf("  %-12s %,d   (CSV write)%n", Phase.SINK.id() + ":", sink);
    System.out.printf(
        "  %-12s %,d   (Flink-only: MiniCluster startup/teardown gap)%n",
        Phase.CLUSTER.id() + ":", cluster);
    System.out.println("derived:");
    System.out.printf(
        "  compute_ms:          %,d   (= aju + aggregate + topk + sink; vs standalone processing)%n",
        compute);
    System.out.printf(
        "  flink_tax_ms:        %,d   (= runtime + cluster; no standalone equivalent)%n", flinkTax);
    System.out.printf(
        "  accounted_ms:        %,d   (= load + compute + flink_tax; gap vs execute_wall: %,d)%n",
        accounted,
        Math.max(0, executeWallMs - accounted));
    System.out.println("==========================================================");
  }
}
