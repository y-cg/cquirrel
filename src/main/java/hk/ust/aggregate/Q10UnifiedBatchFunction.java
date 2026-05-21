package hk.ust.aggregate;

import hk.ust.aju.JoinResult;
import hk.ust.aju.Q10ProcessFunction;
import hk.ust.model.TupleUpdate;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Single-operator Flink path aligned with {@link hk.ust.StandaloneRunner}:
 *
 * <p>AJU join maintenance + in-operator revenue aggregation (no per-delta downstream records) + one
 * top-K sort and CSV write in {@link #close()}.
 */
public class Q10UnifiedBatchFunction
    extends ProcessFunction<TupleUpdate, Q10Aggregator.AggregateResult> {

  private static final String DEFAULT_OUTPUT = "result/flink-q10.csv";

  private transient Q10ProcessFunction aju;
  private transient Q10RevenueAggregator aggregator;
  private transient Collector<JoinResult> joinDeltaCollector;

  private final String outputPath;

  public Q10UnifiedBatchFunction() {
    this(System.getProperty("q10.output", DEFAULT_OUTPUT));
  }

  public Q10UnifiedBatchFunction(String outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    aju = new Q10ProcessFunction();
    aju.open(openContext);
    aggregator = new Q10RevenueAggregator();
    aggregator.open();
    joinDeltaCollector =
        new Collector<JoinResult>() {
          @Override
          public void collect(JoinResult record) {
            aggregator.apply(record);
          }

          @Override
          public void close() {}
        };
  }

  @Override
  public void processElement(
      TupleUpdate update, Context ctx, Collector<Q10Aggregator.AggregateResult> out) {
    aju.processElement(update, null, joinDeltaCollector);
  }

  @Override
  public void close() throws Exception {
    try {
      Q10TopKWriter.writeCsv(aggregator, outputPath);
    } finally {
      super.close();
    }
  }
}
