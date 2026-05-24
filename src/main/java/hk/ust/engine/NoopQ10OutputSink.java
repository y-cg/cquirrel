package hk.ust.engine;

import java.util.List;

/** Output sink for internal shards; the coordinator materializes global results. */
public final class NoopQ10OutputSink implements Q10OutputSink {

  @Override
  public void emitTopK(List<Q10TopKRow> rows, OutputTrigger trigger) {}
}
