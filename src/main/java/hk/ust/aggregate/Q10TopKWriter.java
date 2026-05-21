package hk.ust.aggregate;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Writes Q10 top-20 CSV from final aggregated customer revenue (batch finalization). */
public final class Q10TopKWriter {

  private static final int K = 20;

  private Q10TopKWriter() {}

  public static void writeCsv(Q10RevenueAggregator aggregator, String outputPath) throws Exception {
    Map<Long, Long> revenueByCustomer = aggregator.revenueByCustomer();
    Map<Long, Q10RevenueAggregator.GroupInfo> groupInfo = aggregator.groupInfo();

    List<Map.Entry<Long, Long>> sorted = new ArrayList<>(revenueByCustomer.entrySet());
    sorted.sort(Map.Entry.<Long, Long>comparingByValue().reversed());

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
    System.out.printf("Total customer groups: %d%n", revenueByCustomer.size());
  }
}
