package hk.ust.aggregate;

import hk.ust.engine.Q10TopKRow;
import hk.ust.metrics.Phase;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** Writes Q10 top-20 CSV from final aggregated customer revenue (batch finalization). */
public final class Q10TopKWriter {

  private static final int K = 20;

  private Q10TopKWriter() {}

  public static void writeCsv(Q10RevenueAggregator aggregator, String outputPath) throws Exception {
    writeCsvTimed(aggregator, outputPath, null);
  }

  /** Builds the final top-20 rows from aggregated customer revenue. */
  public static List<Q10TopKRow> buildTopKRows(Q10RevenueAggregator aggregator) {
    List<Map.Entry<Long, Long>> sorted = sortByRevenue(aggregator);
    Map<Long, Q10RevenueAggregator.GroupInfo> groupInfo = aggregator.groupInfo();
    List<Q10TopKRow> rows = new ArrayList<>();
    for (Map.Entry<Long, Long> entry : sorted) {
      if (rows.size() >= K) {
        break;
      }
      long revenue = entry.getValue();
      if (revenue <= 0) {
        break;
      }
      Q10RevenueAggregator.GroupInfo info = groupInfo.get(entry.getKey());
      if (info == null) {
        continue;
      }
      rows.add(
          new Q10TopKRow(
              entry.getKey(),
              info.cName(),
              revenue,
              info.cAcctbal(),
              info.nName(),
              info.cAddress(),
              info.cPhone(),
              info.cComment()));
    }
    return rows;
  }

  public static void writeCsvTimed(
      Q10RevenueAggregator aggregator,
      String outputPath,
      BiConsumer<Phase, Long> phaseTimer)
      throws Exception {
    long t0 = System.nanoTime();
    List<Q10TopKRow> rows = buildTopKRows(aggregator);
    recordPhase(phaseTimer, Phase.TOPK, t0);

    t0 = System.nanoTime();
    writeRows(rows, outputPath, aggregator.revenueByCustomer().size());
    recordPhase(phaseTimer, Phase.SINK, t0);
  }

  private static List<Map.Entry<Long, Long>> sortByRevenue(Q10RevenueAggregator aggregator) {
    List<Map.Entry<Long, Long>> sorted = new ArrayList<>(aggregator.revenueByCustomer().entrySet());
    sorted.sort(Map.Entry.<Long, Long>comparingByValue().reversed());
    return sorted;
  }

  private static void writeRows(List<Q10TopKRow> rows, String outputPath, int totalGroups)
      throws Exception {
    Path path = Path.of(outputPath);
    Files.createDirectories(path.getParent());

    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
      pw.println("c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment");
      for (Q10TopKRow row : rows) {
        pw.printf(
            "%d,%s,%.4f,%.2f,%s,\"%s\",%s,\"%s\"%n",
            row.cCustkey(),
            row.cName(),
            row.revenueCents() / 100.0,
            row.cAcctbalCents() / 100.0,
            row.nName(),
            row.cAddress().replace("\"", "\"\""),
            row.cPhone(),
            row.cComment().replace("\"", "\"\""));
      }
    }

    System.out.println("Q10 top-K results written to: " + path.toAbsolutePath());
    System.out.printf("Total customer groups: %d%n", totalGroups);
  }

  private static void recordPhase(BiConsumer<Phase, Long> phaseTimer, Phase phase, long startNs) {
    if (phaseTimer != null) {
      phaseTimer.accept(phase, System.nanoTime() - startNs);
    }
  }
}
