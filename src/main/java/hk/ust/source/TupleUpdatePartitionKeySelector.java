package hk.ust.source;

import hk.ust.model.TupleUpdate;
import java.io.Serializable;
import org.apache.flink.api.java.functions.KeySelector;

/** Key selector for routing tuple updates to AJU partitions. */
public final class TupleUpdatePartitionKeySelector
    implements KeySelector<TupleUpdate, Long>, Serializable {

  private final String dataDir;

  public TupleUpdatePartitionKeySelector(String dataDir) {
    this.dataDir = dataDir;
  }

  @Override
  public Long getKey(TupleUpdate update) throws Exception {
    OrderCustKeyIndex index = OrderCustKeyIndex.load(java.nio.file.Path.of(dataDir));
    return UpdateRouter.partitionKey(update, index);
  }
}
