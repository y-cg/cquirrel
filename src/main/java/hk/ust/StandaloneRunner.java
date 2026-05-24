package hk.ust;

import hk.ust.engine.CsvQ10OutputSink;
import hk.ust.engine.OutputPolicyConfig;
import hk.ust.engine.ParallelQ10BatchEngine;
import hk.ust.engine.Q10BatchEngine;
import hk.ust.metrics.PhaseTimingAccumulator;
import hk.ust.metrics.Phase;
import hk.ust.metrics.RunMetrics;
import hk.ust.model.*;
import hk.ust.model.TupleUpdate.*;
import hk.ust.source.TpchCsvParser;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Standalone (non-Flink) runner for the AJU Q10 algorithm.
 *
 * <p>Uses {@link Q10BatchEngine} with configurable input batch size and {@link
 * hk.ust.engine.OutputPolicy}.
 */
public class StandaloneRunner {

  public static void main(String[] args) throws Exception {
    String tpchDataDir = System.getenv("TPCH_DATA_DIR");
    if (tpchDataDir == null || tpchDataDir.trim().isEmpty()) {
      throw new IllegalStateException("TPCH_DATA_DIR is not set");
    }

    Path dir = Path.of(tpchDataDir);
    String outputPath = System.getProperty("q10.output", "result/standalone-q10.csv");
    int parallelism = OutputPolicyConfig.parallelism();

    try (RunMetrics metrics = RunMetrics.start("standalone", tpchDataDir)) {
      metrics.setParallelism(parallelism);
      if (parallelism == 1) {
        runSingleThreaded(dir, outputPath, metrics);
      } else {
        runParallel(dir, outputPath, parallelism, metrics);
      }
    }
  }

  private static void runSingleThreaded(Path dir, String outputPath, RunMetrics metrics)
      throws Exception {
    Q10BatchEngine engine =
        new Q10BatchEngine(
            OutputPolicyConfig.outputPolicy(),
            OutputPolicyConfig.inputBatchSize(),
            new CsvQ10OutputSink(outputPath),
            metrics::addPhaseDuration);
    engine.open();

    long updatesTotal = 0;
    System.out.printf(
        "Output policy: %s, input batch size: %s, parallelism: 1%n",
        engine.outputPolicy(),
        engine.inputBatchSize() == Integer.MAX_VALUE ? "unbounded" : engine.inputBatchSize());

    updatesTotal += processTable(dir.resolve("nation.tbl"), "nation", engine, metrics, "nation");
    updatesTotal += processTable(dir.resolve("customer.tbl"), "customer", engine, metrics, "customer");
    updatesTotal += processTable(dir.resolve("orders.tbl"), "orders", engine, metrics, "orders");
    updatesTotal += processTable(dir.resolve("lineitem.tbl"), "lineitem", engine, metrics, "lineitem");

    engine.finish();

    metrics.setUpdatesTotal(updatesTotal);
    metrics.setJoinDeltasTotal(engine.joinDeltasTotal());
    System.out.printf("Bulk load complete: %d join deltas.%n", engine.joinDeltasTotal());
  }

  private static void runParallel(
      Path dir, String outputPath, int parallelism, RunMetrics metrics) throws Exception {
    PhaseTimingAccumulator timings = new PhaseTimingAccumulator();
    try (ParallelQ10BatchEngine engine =
        new ParallelQ10BatchEngine(
            parallelism,
            OutputPolicyConfig.outputPolicy(),
            OutputPolicyConfig.inputBatchSize(),
            new CsvQ10OutputSink(outputPath),
            timings)) {
      engine.open();

      long updatesTotal = 0;
      System.out.printf(
          "Output policy: %s, input batch size: %s, parallelism: %d%n",
          engine.outputPolicy(),
          engine.inputBatchSize() == Integer.MAX_VALUE ? "unbounded" : engine.inputBatchSize(),
          engine.parallelism());

      updatesTotal +=
          processTableParallel(dir.resolve("nation.tbl"), "nation", engine, metrics, "nation");
      updatesTotal +=
          processTableParallel(dir.resolve("customer.tbl"), "customer", engine, metrics, "customer");
      updatesTotal +=
          processTableParallel(dir.resolve("orders.tbl"), "orders", engine, metrics, "orders");
      updatesTotal +=
          processTableParallel(dir.resolve("lineitem.tbl"), "lineitem", engine, metrics, "lineitem");

      engine.finish();
      timings.flushTo(metrics);

      metrics.setUpdatesTotal(updatesTotal);
      metrics.setJoinDeltasTotal(engine.joinDeltasTotal());
      System.out.printf("Bulk load complete: %d join deltas.%n", engine.joinDeltasTotal());
    }
  }

  private static long processTable(
      Path file, String relation, Q10BatchEngine engine, RunMetrics metrics, String label)
      throws Exception {
    if (!Files.exists(file)) {
      return 0;
    }

    System.out.println("Loading " + label + "...");
    long updates = 0;

    try (BufferedReader reader = Files.newBufferedReader(file)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }

        long t0 = System.nanoTime();
        TupleUpdate update = parseLine(line, relation, UpdateType.INSERT);
        metrics.addPhaseDuration(Phase.LOAD, System.nanoTime() - t0);

        if (update != null) {
          engine.processUpdate(update);
          updates++;
          if (updates % 100_000 == 0) {
            metrics.sampleMemory();
          }
        }
      }
    }

    engine.endInputBatch();
    return updates;
  }

  private static long processTableParallel(
      Path file, String relation, ParallelQ10BatchEngine engine, RunMetrics metrics, String label)
      throws Exception {
    if (!Files.exists(file)) {
      return 0;
    }

    System.out.println("Loading " + label + "...");
    long updates = 0;

    try (BufferedReader reader = Files.newBufferedReader(file)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }

        long t0 = System.nanoTime();
        TupleUpdate update = parseLine(line, relation, UpdateType.INSERT);
        metrics.addPhaseDuration(Phase.LOAD, System.nanoTime() - t0);

        if (update != null) {
          engine.processUpdate(update);
          updates++;
          if (updates % 100_000 == 0) {
            metrics.sampleMemory();
          }
        }
      }
    }

    engine.endInputBatch();
    return updates;
  }

  private static TupleUpdate parseLine(String line, String relation, UpdateType type) {
    return switch (relation) {
      case "nation" -> new NationUpdate(type, TpchCsvParser.parseNation(line));
      case "customer" -> new CustomerUpdate(type, TpchCsvParser.parseCustomer(line));
      case "orders" -> new OrdersUpdate(type, TpchCsvParser.parseOrders(line));
      case "lineitem" -> new LineitemUpdate(type, TpchCsvParser.parseLineitem(line));
      default -> null;
    };
  }
}
