package hk.ust;

import hk.ust.aju.JoinResult;
import hk.ust.aju.Q10ProcessFunction;
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

    // =====================================================================
    // Initialize the AJU process function (bypassing Flink lifecycle)
    // =====================================================================

    Q10ProcessFunction processor = new Q10ProcessFunction();
    processor.open(null);

    // Collect join deltas
    List<JoinResult> deltas = new ArrayList<>();
    var collector = new SimpleCollector<JoinResult>(deltas);

    // =====================================================================
    // Phase 1: Bulk load in bottom-up order
    // =====================================================================

    long startTime = System.nanoTime();

    System.out.println("Loading nation...");
    processTable(dir.resolve("nation.tbl"), "nation", processor, collector);

    System.out.println("Loading customer...");
    processTable(dir.resolve("customer.tbl"), "customer", processor, collector);

    System.out.println("Loading orders...");
    processTable(dir.resolve("orders.tbl"), "orders", processor, collector);

    System.out.println("Loading lineitem...");
    processTable(dir.resolve("lineitem.tbl"), "lineitem", processor, collector);

    long loadTime = System.nanoTime() - startTime;
    System.out.printf(
        "Bulk load complete: %d join deltas in %.3f seconds.%n", deltas.size(), loadTime / 1e9);

    // =====================================================================
    // Aggregate: compute SUM(revenue) per customer group
    // =====================================================================

    Map<Long, Long> revenueByCustomer = new HashMap<>();
    Map<Long, JoinResult> customerInfo = new HashMap<>();

    for (JoinResult jr : deltas) {
      if (jr.type() == UpdateType.INSERT) {
        revenueByCustomer.merge(jr.cCustkey(), jr.revenue(), Long::sum);
        customerInfo.putIfAbsent(jr.cCustkey(), jr);
      } else {
        revenueByCustomer.merge(jr.cCustkey(), -jr.revenue(), Long::sum);
      }
    }

    // =====================================================================
    // Top-20 by revenue descending — write CSV output
    // =====================================================================

    List<Map.Entry<Long, Long>> sorted = new ArrayList<>(revenueByCustomer.entrySet());
    sorted.sort(Map.Entry.<Long, Long>comparingByValue().reversed());

    String outputPath = System.getProperty("q10.output", "result/standalone-q10.csv");
    Path outFile = Path.of(outputPath);
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

    System.out.println("Q10 top-K results written to: " + outFile.toAbsolutePath());
    System.out.printf("Total customer groups: %d%n", revenueByCustomer.size());
  }

  private static void processTable(
      Path file,
      String relation,
      Q10ProcessFunction processor,
      SimpleCollector<JoinResult> collector)
      throws Exception {
    if (!Files.exists(file)) return;
    try (BufferedReader reader = Files.newBufferedReader(file)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) continue;
        TupleUpdate update = parseLine(line, relation, UpdateType.INSERT);
        if (update != null) {
          processor.processElement(update, null, collector);
        }
      }
    }
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
