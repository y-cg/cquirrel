package hk.ust.aggregate;

import java.util.Collection;
import java.util.Map;

/** Merges independently maintained Q10 revenue aggregates into one global aggregate. */
public final class Q10AggregateMerger {

  private Q10AggregateMerger() {}

  public static Q10RevenueAggregator merge(Collection<Q10RevenueAggregator> shardAggregates) {
    Q10RevenueAggregator merged = new Q10RevenueAggregator();
    merged.open();

    for (Q10RevenueAggregator shard : shardAggregates) {
      Map<Long, Q10RevenueAggregator.GroupInfo> groupInfo = shard.groupInfo();
      for (Map.Entry<Long, Long> entry : shard.revenueByCustomer().entrySet()) {
        merged.mergeGroup(entry.getKey(), entry.getValue(), groupInfo.get(entry.getKey()));
      }
    }

    return merged;
  }
}
