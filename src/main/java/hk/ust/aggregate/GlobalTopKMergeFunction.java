package hk.ust.aggregate;

import hk.ust.aggregate.Q10Aggregator.AggregateResult;
import hk.ust.metrics.FlinkOperatorTimings;
import hk.ust.metrics.Phase;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Global top-K merge for split/partitioned pipelines. Receives aggregate updates from all
 * subtasks and writes the final top-20 CSV at job end.
 */
public class GlobalTopKMergeFunction extends ProcessFunction<AggregateResult, AggregateResult> {

  private static final int K = 20;
  private static final String DEFAULT_OUTPUT = "result/flink-q10.csv";

  private final String outputPath;

  private transient TreeMap<Long, Set<Long>> revenueIndex;
  private transient Map<Long, Long> currentRevenue;
  private transient Map<Long, AggregateResult> latestResult;

  public GlobalTopKMergeFunction() {
    this(System.getProperty("q10.output", DEFAULT_OUTPUT));
  }

  public GlobalTopKMergeFunction(String outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public void open(OpenContext openContext) {
    revenueIndex = new TreeMap<>(Comparator.reverseOrder());
    currentRevenue = new HashMap<>();
    latestResult = new HashMap<>();
  }

  @Override
  public void processElement(AggregateResult result, Context ctx, Collector<AggregateResult> out) {
    long t0 = System.nanoTime();
    long custkey = result.cCustkey();
    long newRevenue = result.revenue();

    Long oldRevenue = currentRevenue.get(custkey);
    if (oldRevenue != null) {
      Set<Long> keys = revenueIndex.get(oldRevenue);
      if (keys != null) {
        keys.remove(custkey);
        if (keys.isEmpty()) {
          revenueIndex.remove(oldRevenue);
        }
      }
    }

    if (newRevenue <= 0) {
      currentRevenue.remove(custkey);
      latestResult.remove(custkey);
    } else {
      currentRevenue.put(custkey, newRevenue);
      revenueIndex.computeIfAbsent(newRevenue, ignored -> new HashSet<>()).add(custkey);
      latestResult.put(custkey, result);
    }
    FlinkOperatorTimings.add(Phase.TOPK, System.nanoTime() - t0);
  }

  @Override
  public void close() throws Exception {
    long t0 = System.nanoTime();
    Path path = Path.of(outputPath);
    Files.createDirectories(path.getParent());

    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
      pw.println("c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment");

      int count = 0;
      for (Map.Entry<Long, Set<Long>> entry : revenueIndex.entrySet()) {
        for (long key : entry.getValue()) {
          AggregateResult row = latestResult.get(key);
          if (row != null) {
            pw.printf(
                "%d,%s,%.4f,%.2f,%s,\"%s\",%s,\"%s\"%n",
                row.cCustkey(),
                row.cName(),
                row.revenue() / 100.0,
                row.cAcctbal() / 100.0,
                row.nName(),
                row.cAddress().replace("\"", "\"\""),
                row.cPhone(),
                row.cComment().replace("\"", "\"\""));
            count++;
            if (count >= K) {
              break;
            }
          }
        }
        if (count >= K) {
          break;
        }
      }
    }
    FlinkOperatorTimings.add(Phase.TOPK, System.nanoTime() - t0);

    t0 = System.nanoTime();
    System.out.println("Q10 top-K results written to: " + path.toAbsolutePath());
    System.out.printf("Total customer groups tracked: %d%n", latestResult.size());
    FlinkOperatorTimings.add(Phase.SINK, System.nanoTime() - t0);
  }
}
