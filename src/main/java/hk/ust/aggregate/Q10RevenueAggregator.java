package hk.ust.aggregate;

import hk.ust.aju.JoinResult;
import hk.ust.model.UpdateType;
import java.util.HashMap;
import java.util.Map;

/**
 * In-memory incremental revenue aggregation for Q10 (no per-update emits).
 *
 * <p>Used by the unified Flink batch operator and standalone-style finalization paths.
 */
public class Q10RevenueAggregator {

  private transient Map<Long, Long> revenueByCustomer;
  private transient Map<Long, GroupInfo> groupInfo;
  private transient Map<Long, Integer> countByCustomer;

  record GroupInfo(
      String cName, long cAcctbal, String nName, String cAddress, String cPhone, String cComment) {}

  public void open() {
    revenueByCustomer = new HashMap<>();
    groupInfo = new HashMap<>();
    countByCustomer = new HashMap<>();
  }

  /** Applies one join delta to running per-customer sums (O(1)). */
  public void apply(JoinResult jr) {
    long custkey = jr.cCustkey();

    if (jr.type() == UpdateType.INSERT) {
      revenueByCustomer.merge(custkey, jr.revenue(), Long::sum);
      countByCustomer.merge(custkey, 1, Integer::sum);
      groupInfo.putIfAbsent(
          custkey,
          new GroupInfo(
              jr.cName(), jr.cAcctbal(), jr.nName(), jr.cAddress(), jr.cPhone(), jr.cComment()));
    } else {
      revenueByCustomer.merge(custkey, -jr.revenue(), Long::sum);
      int newCount = countByCustomer.merge(custkey, -1, Integer::sum);
      if (newCount <= 0) {
        revenueByCustomer.remove(custkey);
        countByCustomer.remove(custkey);
        groupInfo.remove(custkey);
      }
    }
  }

  public Map<Long, Long> revenueByCustomer() {
    return revenueByCustomer;
  }

  public Map<Long, GroupInfo> groupInfo() {
    return groupInfo;
  }
}
