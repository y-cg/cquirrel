package hk.ust.engine;

import java.util.List;

/** Consumer of materialized Q10 top-K snapshots. */
@FunctionalInterface
public interface Q10OutputSink {

  void emitTopK(List<Q10TopKRow> rows, OutputTrigger trigger) throws Exception;
}
