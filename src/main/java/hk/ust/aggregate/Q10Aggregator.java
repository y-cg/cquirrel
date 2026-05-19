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
 * Since SUM supports additive inverses, we maintain the running sum with O(1) per update: - INSERT
 * delta: revenue[custkey] += contribution - DELETE delta: revenue[custkey] -= contribution
 *
 * <p>Emits AggregateResult records representing the current state of each group whenever it
 * changes.
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

  // Per-group state: running revenue sum
  private transient Map<Long, Long> revenueByCustomer;

  // Per-group metadata (stable across updates; set on first insert)
  private transient Map<Long, GroupInfo> groupInfo;

  // Count of contributing lineitem tuples per group (to know when to remove)
  private transient Map<Long, Integer> countByCustomer;

  private record GroupInfo(
      String cName, long cAcctbal, String nName, String cAddress, String cPhone, String cComment) {}

  @Override
  public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
    revenueByCustomer = new HashMap<>();
    groupInfo = new HashMap<>();
    countByCustomer = new HashMap<>();
  }

  @Override
  public void processElement(JoinResult jr, Context ctx, Collector<AggregateResult> out) {
    long custkey = jr.cCustkey();

    if (jr.type() == UpdateType.INSERT) {
      // Positive delta: add contribution
      revenueByCustomer.merge(custkey, jr.revenue(), Long::sum);
      countByCustomer.merge(custkey, 1, Integer::sum);

      // Store group metadata on first encounter
      groupInfo.putIfAbsent(
          custkey,
          new GroupInfo(
              jr.cName(), jr.cAcctbal(), jr.nName(), jr.cAddress(), jr.cPhone(), jr.cComment()));
    } else {
      // Negative delta: subtract contribution
      revenueByCustomer.merge(custkey, -jr.revenue(), Long::sum);
      int newCount = countByCustomer.merge(custkey, -1, Integer::sum);

      if (newCount <= 0) {
        // No more contributing lineitems; remove the group entirely
        revenueByCustomer.remove(custkey);
        countByCustomer.remove(custkey);
        groupInfo.remove(custkey);
        return; // no output for removed group
      }
    }

    // Emit current state of this group
    GroupInfo info = groupInfo.get(custkey);
    if (info != null) {
      out.collect(
          new AggregateResult(
              custkey,
              info.cName(),
              revenueByCustomer.getOrDefault(custkey, 0L),
              info.cAcctbal(),
              info.nName(),
              info.cAddress(),
              info.cPhone(),
              info.cComment()));
    }
  }

  // =========================================================================
  // For verification: retrieve the final aggregated state
  // =========================================================================

  public Map<Long, Long> getRevenueByCustomer() {
    return revenueByCustomer;
  }
}
