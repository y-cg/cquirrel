package hk.ust;

import hk.ust.aggregate.GlobalTopKMergeFunction;
import hk.ust.aggregate.PartitionedQ10AjuFunction;
import hk.ust.aggregate.Q10AjuFunction;
import hk.ust.aggregate.Q10Aggregator;
import hk.ust.aggregate.Q10UnifiedBatchFunction;
import hk.ust.aggregate.TimedQ10Aggregator;
import hk.ust.aju.JoinResult;
import hk.ust.engine.ParallelismConfig;
import hk.ust.engine.ParallelismConfig.PipelineMode;
import hk.ust.metrics.RunMetrics;
import hk.ust.model.TupleUpdate;
import hk.ust.source.PartitionedTpchUpdateSource;
import hk.ust.source.TpchUpdateSource;
import hk.ust.source.TupleUpdatePartitionKeySelector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink;

/** Builds Flink AJU Q10 pipelines for unified, split, and partitioned benchmark modes. */
final class AjuPipelineBuilder {

  private AjuPipelineBuilder() {}

  static void wire(
      StreamExecutionEnvironment env,
      String tpchDataDir,
      RunMetrics metrics,
      String outputPath) {

    int globalP = ParallelismConfig.global();
    int ajuP = ParallelismConfig.isPartitionedAju() ? ParallelismConfig.ajuParallelism() : 1;
    int aggP = ParallelismConfig.aggParallelism();
    PipelineMode mode = ParallelismConfig.pipelineMode();

    env.setParallelism(globalP);
    metrics.setParallelism(globalP);
    metrics.setAjuParallelism(ajuP);
    metrics.setAggParallelism(aggP);
    metrics.setPipelineMode(mode.name().toLowerCase());

    System.out.printf(
        "Pipeline mode=%s globalParallelism=%d ajuParallelism=%d aggParallelism=%d%n",
        mode, globalP, ajuP, aggP);

    switch (mode) {
      case UNIFIED -> wireUnified(env, tpchDataDir, outputPath);
      case SPLIT -> wireSplit(env, tpchDataDir, outputPath, ajuP, aggP);
      case PARTITIONED -> wirePartitioned(env, tpchDataDir, outputPath, ajuP, aggP);
    }
  }

  private static void wireUnified(
      StreamExecutionEnvironment env, String tpchDataDir, String outputPath) {
    DataStream<TupleUpdate> updates = sourceStream(env, tpchDataDir);

    updates
        .process(new Q10UnifiedBatchFunction(outputPath))
        .name("aju-q10-unified-batch")
        .setParallelism(1)
        .addSink(new DiscardingSink<Q10Aggregator.AggregateResult>())
        .name("discarding-sink")
        .setParallelism(1);
  }

  private static void wireSplit(
      StreamExecutionEnvironment env,
      String tpchDataDir,
      String outputPath,
      int ajuP,
      int aggP) {
    DataStream<TupleUpdate> updates = sourceStream(env, tpchDataDir);

    SingleOutputStreamOperator<JoinResult> deltas =
        updates
            .process(new Q10AjuFunction())
            .name("aju-q10-split")
            .setParallelism(ajuP);

    wireAggregation(deltas, outputPath, aggP);
  }

  private static void wirePartitioned(
      StreamExecutionEnvironment env,
      String tpchDataDir,
      String outputPath,
      int ajuP,
      int aggP) {
    DataStream<TupleUpdate> updates =
        env.addSource(new PartitionedTpchUpdateSource(tpchDataDir), "partitioned-tpch-source")
            .returns(TypeInformation.of(TupleUpdate.class))
            .setParallelism(1);

    SingleOutputStreamOperator<JoinResult> deltas =
        updates
            .keyBy(new TupleUpdatePartitionKeySelector(tpchDataDir))
            .process(new PartitionedQ10AjuFunction(tpchDataDir))
            .name("aju-q10-partitioned")
            .setParallelism(ajuP);

    wireAggregation(deltas, outputPath, aggP);
  }

  private static DataStream<TupleUpdate> sourceStream(
      StreamExecutionEnvironment env, String tpchDataDir) {
    return env.addSource(new TpchUpdateSource(tpchDataDir), "tpch-update-source")
        .returns(TypeInformation.of(TupleUpdate.class))
        .setParallelism(1);
  }

  private static void wireAggregation(
      SingleOutputStreamOperator<JoinResult> deltas, String outputPath, int aggP) {
    SingleOutputStreamOperator<Q10Aggregator.AggregateResult> aggregated =
        deltas
            .keyBy(JoinResult::cCustkey)
            .process(new TimedQ10Aggregator())
            .name("q10-aggregator")
            .setParallelism(aggP);

    aggregated
        .rebalance()
        .process(new GlobalTopKMergeFunction(outputPath))
        .name("global-topk-merge")
        .setParallelism(1)
        .addSink(new DiscardingSink<Q10Aggregator.AggregateResult>())
        .name("discarding-sink")
        .setParallelism(1);
  }
}
