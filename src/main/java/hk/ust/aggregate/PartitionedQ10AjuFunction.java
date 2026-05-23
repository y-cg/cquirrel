package hk.ust.aggregate;

import hk.ust.aju.JoinResult;
import hk.ust.aju.Q10ProcessFunction;
import hk.ust.metrics.FlinkOperatorTimings;
import hk.ust.metrics.Phase;
import hk.ust.model.CustomerTuple;
import hk.ust.model.TupleUpdate;
import hk.ust.model.TupleUpdate.CustomerUpdate;
import hk.ust.model.TupleUpdate.NationUpdate;
import hk.ust.model.UpdateType;
import hk.ust.source.TpchCsvParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Partitioned AJU operator: bootstraps nation and local customers, then processes routed
 * orders/lineitem updates for this subtask.
 */
public class PartitionedQ10AjuFunction extends ProcessFunction<TupleUpdate, JoinResult> {

  private final String dataDir;
  private transient Q10ProcessFunction aju;

  public PartitionedQ10AjuFunction(String dataDir) {
    this.dataDir = dataDir;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

    aju = new Q10ProcessFunction();
    aju.open(openContext);

    long t0 = System.nanoTime();
    bootstrapSharedDimensionTables(Path.of(dataDir));
    FlinkOperatorTimings.add(Phase.AJU, System.nanoTime() - t0);

    System.out.printf(
        "PartitionedQ10AjuFunction subtask=%d/%d bootstrapped shared tables from %s%n",
        subtaskIndex, parallelism, dataDir);
  }

  @Override
  public void processElement(TupleUpdate update, Context ctx, Collector<JoinResult> out) {
    long t0 = System.nanoTime();
    aju.processElement(update, ctx, JoinResultCollectors.emitting(out));
    FlinkOperatorTimings.add(Phase.AJU, System.nanoTime() - t0);
  }

  /**
   * Replicates nation and customer bulk rows on every subtask so join assembly matches Flink's
   * key-group routing for orders/lineitem.
   */
  private void bootstrapSharedDimensionTables(Path dir) throws IOException {
    Collector<JoinResult> metricsCollector = JoinResultCollectors.metricsOnly();

    Path nationFile = dir.resolve("nation.tbl");
    if (Files.exists(nationFile)) {
      try (BufferedReader reader = Files.newBufferedReader(nationFile)) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.isBlank()) {
            continue;
          }
          aju.processElement(
              new NationUpdate(UpdateType.INSERT, TpchCsvParser.parseNation(line)),
              null,
              metricsCollector);
        }
      }
    }

    Path customerFile = dir.resolve("customer.tbl");
    if (Files.exists(customerFile)) {
      try (BufferedReader reader = Files.newBufferedReader(customerFile)) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.isBlank()) {
            continue;
          }
          CustomerTuple tuple = TpchCsvParser.parseCustomer(line);
          aju.processElement(
              new CustomerUpdate(UpdateType.INSERT, tuple), null, metricsCollector);
        }
      }
    }
  }
}
