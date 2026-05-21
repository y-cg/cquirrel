package hk.ust.aggregate;

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

  public static void writeCsvTimed(
      Q10RevenueAggregator aggregator,
      String outputPath,
      BiConsumer<Phase, Long> phaseTimer)
      throws Exception {
    long t0 = System.nanoTime();
    List<Map.Entry<Long, Long>> sorted = sortByRevenue(aggregator);
    recordPhase(phaseTimer, Phase.TOPK, t0);

    t0 = System.nanoTime();
    writeSortedCsv(sorted, aggregator, outputPath);
    recordPhase(phaseTimer, Phase.SINK, t0);
  }

  private static List<Map.Entry<Long, Long>> sortByRevenue(Q10RevenueAggregator aggregator) {
    List<Map.Entry<Long, Long>> sorted = new ArrayList<>(aggregator.revenueByCustomer().entrySet());
    sorted.sort(Map.Entry.<Long, Long>comparingByValue().reversed());
    return sorted;
  }

  private static void writeSortedCsv(
      List<Map.Entry<Long, Long>> sorted, Q10RevenueAggregator aggregator, String outputPath)
      throws Exception {
    Map<Long, Q10RevenueAggregator.GroupInfo> groupInfo = aggregator.groupInfo();
    Path path = Path.of(outputPath);
    Files.createDirectories(path.getParent());

    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
      pw.println("c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment");

      int count = 0;
      for (Map.Entry<Long, Long> entry : sorted) {
        if (count >= K) {
          break;
        }
        long custkey = entry.getKey();
        long revenue = entry.getValue();
        if (revenue <= 0) {
          break;
        }

        Q10RevenueAggregator.GroupInfo info = groupInfo.get(custkey);
        if (info == null) {
          continue;
        }

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

    System.out.println("Q10 top-K results written to: " + path.toAbsolutePath());
    System.out.printf("Total customer groups: %d%n", aggregator.revenueByCustomer().size());
  }

  private static void recordPhase(BiConsumer<Phase, Long> phaseTimer, Phase phase, long startNs) {
    if (phaseTimer != null) {
      phaseTimer.accept(phase, System.nanoTime() - startNs);
    }
  }
}
