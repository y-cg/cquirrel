package hk.ust.aggregate;

import hk.ust.aju.JoinResult;
import hk.ust.model.UpdateType;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Incremental aggregator for Q10's GROUP BY ... SUM(revenue).
 *
 * <p>Each JoinResult represents a single lineitem's contribution to its customer's revenue group.
 * Since SUM supports additive inverses, we maintain the running sum with O(1) per update.
 *
 * <p>For Flink batch runs, prefer {@link Q10UnifiedBatchFunction} which aggregates in-operator
 * without per-delta downstream records.
 */
public class Q10Aggregator extends ProcessFunction<JoinResult, Q10Aggregator.AggregateResult> {

  /** The aggregated result for one customer group. */
  public record AggregateResult(
      long cCustkey,
      String cName,
      long revenue, // current total revenue in cents
      long cAcctbal, // in cents
      String nName,
      String cAddress,
      String cPhone,
      String cComment)
      implements Serializable {}

  private transient Q10RevenueAggregator revenueAggregator;

  @Override
  public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
    revenueAggregator = new Q10RevenueAggregator();
    revenueAggregator.open();
  }

  @Override
  public void processElement(JoinResult jr, Context ctx, Collector<AggregateResult> out) {
    revenueAggregator.apply(jr);

    long custkey = jr.cCustkey();
    Q10RevenueAggregator.GroupInfo info = revenueAggregator.groupInfo().get(custkey);
    if (info == null) {
      return;
    }
    long revenue = revenueAggregator.revenueByCustomer().getOrDefault(custkey, 0L);
    if (revenue <= 0) {
      return;
    }

    out.collect(
        new AggregateResult(
            custkey,
            info.cName(),
            revenue,
            info.cAcctbal(),
            info.nName(),
            info.cAddress(),
            info.cPhone(),
            info.cComment()));
  }

  public Map<Long, Long> getRevenueByCustomer() {
    return revenueAggregator.revenueByCustomer();
  }
}
