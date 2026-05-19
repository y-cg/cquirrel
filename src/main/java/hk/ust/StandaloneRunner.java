package hk.ust;

import hk.ust.aju.JoinResult;
import hk.ust.aju.Q10ProcessFunction;
import hk.ust.metrics.Phase;
import hk.ust.metrics.RunMetrics;
import hk.ust.model.*;
import hk.ust.model.TupleUpdate.*;
import hk.ust.source.TpchCsvParser;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Standalone (non-Flink) runner for the AJU Q10 algorithm.
 *
 * <p>Runs the algorithm in a single thread without Flink's distributed runtime, matching the
 * paper's "single-thread standalone mode" evaluation. This is useful for correctness verification
 * and profiling without Flink overhead.
 *
 * <p>Usage: java -cp ... hk.ust.StandaloneRunner (requires TPCH_DATA_DIR environment variable)
 */
public class StandaloneRunner {

  public static void main(String[] args) throws Exception {
    String tpchDataDir = System.getenv("TPCH_DATA_DIR");
    if (tpchDataDir == null || tpchDataDir.trim().isEmpty()) {
      throw new IllegalStateException("TPCH_DATA_DIR is not set");
    }

    Path dir = Path.of(tpchDataDir);

    try (RunMetrics metrics = RunMetrics.start("standalone", tpchDataDir)) {
      Q10ProcessFunction processor = new Q10ProcessFunction();
      processor.open(null);

      List<JoinResult> deltas = new ArrayList<>();
      var collector = new SimpleCollector<JoinResult>(deltas);

      long updatesTotal = 0;

      System.out.println("Loading nation...");
      updatesTotal +=
          processTable(dir.resolve("nation.tbl"), "nation", processor, collector, metrics);

      System.out.println("Loading customer...");
      updatesTotal +=
          processTable(dir.resolve("customer.tbl"), "customer", processor, collector, metrics);

      System.out.println("Loading orders...");
      updatesTotal +=
          processTable(dir.resolve("orders.tbl"), "orders", processor, collector, metrics);

      System.out.println("Loading lineitem...");
      updatesTotal +=
          processTable(dir.resolve("lineitem.tbl"), "lineitem", processor, collector, metrics);

      System.out.printf("Bulk load complete: %d join deltas.%n", deltas.size());

      metrics.setUpdatesTotal(updatesTotal);
      metrics.setJoinDeltasTotal(deltas.size());

      Map<Long, Long> revenueByCustomer;
      Map<Long, JoinResult> customerInfo;
      try (var phase = metrics.phase(Phase.AGGREGATE)) {
        revenueByCustomer = new HashMap<>();
        customerInfo = new HashMap<>();

        for (JoinResult jr : deltas) {
          if (jr.type() == UpdateType.INSERT) {
            revenueByCustomer.merge(jr.cCustkey(), jr.revenue(), Long::sum);
            customerInfo.putIfAbsent(jr.cCustkey(), jr);
          } else {
            revenueByCustomer.merge(jr.cCustkey(), -jr.revenue(), Long::sum);
          }
        }
      }

      String outputPath = System.getProperty("q10.output", "result/standalone-q10.csv");
      Path outFile = Path.of(outputPath);

      List<Map.Entry<Long, Long>> sorted;
      try (var topkPhase = metrics.phase(Phase.TOPK)) {
        sorted = new ArrayList<>(revenueByCustomer.entrySet());
        sorted.sort(Map.Entry.<Long, Long>comparingByValue().reversed());
      }

      try (var sinkPhase = metrics.phase(Phase.SINK)) {
        Files.createDirectories(outFile.getParent());

        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(outFile))) {
          pw.println("c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment");

          int count = 0;
          for (Map.Entry<Long, Long> entry : sorted) {
            if (count >= 20) break;
            long custkey = entry.getKey();
            long revenue = entry.getValue();
            if (revenue <= 0) break;

            JoinResult info = customerInfo.get(custkey);
            if (info == null) continue;

            pw.printf(
                "%d,%s,%.4f,%.2f,%s,\"%s\",%s,\"%s\"%n",
                custkey,
                info.cName(),
                revenue / 100.0,
                info.cAcctbal() / 100.0,
                info.nName(),
                info.cAddress().replace("\"", "\"\""),
                info.cPhone(),
                info.cComment().replace("\"", "\"\""));
            count++;
          }
        }
      }

      System.out.println("Q10 top-K results written to: " + outFile.toAbsolutePath());
      System.out.printf("Total customer groups: %d%n", revenueByCustomer.size());
    }
  }

  private static long processTable(
      Path file,
      String relation,
      Q10ProcessFunction processor,
      SimpleCollector<JoinResult> collector,
      RunMetrics metrics)
      throws Exception {
    if (!Files.exists(file)) {
      return 0;
    }

    long updates = 0;
    long loadNs = 0;
    long ajuNs = 0;

    try (BufferedReader reader = Files.newBufferedReader(file)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }

        TupleUpdate update;
        long t0 = System.nanoTime();
        update = parseLine(line, relation, UpdateType.INSERT);
        loadNs += System.nanoTime() - t0;

        if (update != null) {
          t0 = System.nanoTime();
          processor.processElement(update, null, collector);
          ajuNs += System.nanoTime() - t0;
          updates++;
          if (updates % 100_000 == 0) {
            metrics.sampleMemory();
          }
        }
      }
    }

    metrics.addPhaseDuration(Phase.LOAD, loadNs);
    metrics.addPhaseDuration(Phase.AJU, ajuNs);
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

  /** Minimal Collector implementation that just appends to a list. */
  private static class SimpleCollector<T> implements org.apache.flink.util.Collector<T> {

    private final List<T> results;

    SimpleCollector(List<T> results) {
      this.results = results;
    }

    @Override
    public void collect(T record) {
      results.add(record);
    }

    @Override
    public void close() {}
  }
}
