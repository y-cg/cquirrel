package hk.ust.aggregate;

import hk.ust.aggregate.Q10Aggregator.AggregateResult;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Maintains the top-K results ordered by revenue descending.
 *
 * <p>Uses a TreeMap to efficiently track the top 20 customers by revenue. On each aggregate update,
 * updates the customer's position and emits the current top-K if it changed.
 *
 * <p>For Q10: ORDER BY revenue DESC LIMIT 20.
 */
public class TopKMaintainer extends ProcessFunction<AggregateResult, AggregateResult> {

  private static final int K = 20;
  private static final String DEFAULT_OUTPUT = "result/flink-q10.csv";

  private final String outputPath;

  public TopKMaintainer() {
    this(System.getProperty("q10.output", DEFAULT_OUTPUT));
  }

  public TopKMaintainer(String outputPath) {
    this.outputPath = outputPath;
  }

  // Revenue -> set of custkeys at that revenue level (descending order)
  private transient TreeMap<Long, Set<Long>> revenueIndex;

  // custkey -> current revenue (for removing old entry on update)
  private transient Map<Long, Long> currentRevenue;

  // custkey -> latest full result (for output)
  private transient Map<Long, AggregateResult> latestResult;

  @Override
  public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
    revenueIndex = new TreeMap<>(Comparator.reverseOrder()); // descending
    currentRevenue = new HashMap<>();
    latestResult = new HashMap<>();
  }

  @Override
  public void processElement(AggregateResult result, Context ctx, Collector<AggregateResult> out) {
    long custkey = result.cCustkey();
    long newRevenue = result.revenue();

    // Remove old position if this customer was already tracked
    Long oldRevenue = currentRevenue.get(custkey);
    if (oldRevenue != null) {
      Set<Long> keys = revenueIndex.get(oldRevenue);
      if (keys != null) {
        keys.remove(custkey);
        if (keys.isEmpty()) revenueIndex.remove(oldRevenue);
      }
    }

    if (newRevenue <= 0) {
      // Customer no longer has revenue; remove entirely
      currentRevenue.remove(custkey);
      latestResult.remove(custkey);
    } else {
      // Insert at new position
      currentRevenue.put(custkey, newRevenue);
      revenueIndex.computeIfAbsent(newRevenue, k -> new HashSet<>()).add(custkey);
      latestResult.put(custkey, result);
    }

    // Top-K is finalized once in close() for batch-style runs; no per-update downstream emits.
  }

  @Override
  public void close() throws Exception {
    Path path = Path.of(outputPath);
    Files.createDirectories(path.getParent());

    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
      pw.println("c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment");

      int count = 0;
      for (Map.Entry<Long, Set<Long>> entry : revenueIndex.entrySet()) {
        for (long key : entry.getValue()) {
          AggregateResult r = latestResult.get(key);
          if (r != null) {
            pw.printf(
                "%d,%s,%.4f,%.2f,%s,\"%s\",%s,\"%s\"%n",
                r.cCustkey(),
                r.cName(),
                r.revenue() / 100.0,
                r.cAcctbal() / 100.0,
                r.nName(),
                r.cAddress().replace("\"", "\"\""),
                r.cPhone(),
                r.cComment().replace("\"", "\"\""));
            count++;
            if (count >= K) break;
          }
        }
        if (count >= K) break;
      }
    }

    System.out.println("Q10 top-K results written to: " + path.toAbsolutePath());
  }

  /** Get the current top-K results as a list (for verification). */
  public List<AggregateResult> getTopK() {
    List<AggregateResult> results = new ArrayList<>();
    int count = 0;
    for (Map.Entry<Long, Set<Long>> entry : revenueIndex.entrySet()) {
      for (long key : entry.getValue()) {
        AggregateResult r = latestResult.get(key);
        if (r != null) {
          results.add(r);
          count++;
          if (count >= K) return results;
        }
      }
      if (count >= K) break;
    }
    return results;
  }
}
