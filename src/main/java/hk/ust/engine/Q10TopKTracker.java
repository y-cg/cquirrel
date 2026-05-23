package hk.ust.engine;

import hk.ust.aggregate.Q10RevenueAggregator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Incrementally maintains Q10 top-20 by revenue for streaming output policies. */
public final class Q10TopKTracker {

  private static final int K = 20;

  private final TreeMap<Long, Set<Long>> revenueIndex = new TreeMap<>(Comparator.reverseOrder());
  private final Map<Long, Long> currentRevenue = new HashMap<>();
  private final Map<Long, Q10TopKRow> latestRow = new HashMap<>();

  /** Updates one customer group and returns whether the top-K set changed. */
  public boolean update(long custkey, long newRevenue, Q10RevenueAggregator.GroupInfo info) {
    List<Q10TopKRow> before = snapshot();

    Long oldRevenue = currentRevenue.get(custkey);
    if (oldRevenue != null) {
      Set<Long> keys = revenueIndex.get(oldRevenue);
      if (keys != null) {
        keys.remove(custkey);
        if (keys.isEmpty()) {
          revenueIndex.remove(oldRevenue);
        }
      }
    }

    if (newRevenue <= 0 || info == null) {
      currentRevenue.remove(custkey);
      latestRow.remove(custkey);
    } else {
      currentRevenue.put(custkey, newRevenue);
      revenueIndex.computeIfAbsent(newRevenue, ignored -> new HashSet<>()).add(custkey);
      latestRow.put(
          custkey,
          new Q10TopKRow(
              custkey,
              info.cName(),
              newRevenue,
              info.cAcctbal(),
              info.nName(),
              info.cAddress(),
              info.cPhone(),
              info.cComment()));
    }

    return !sameTopKKeys(before, snapshot());
  }

  public List<Q10TopKRow> snapshot() {
    List<Q10TopKRow> rows = new ArrayList<>(K);
    for (Map.Entry<Long, Set<Long>> entry : revenueIndex.entrySet()) {
      for (long custkey : entry.getValue()) {
        Q10TopKRow row = latestRow.get(custkey);
        if (row != null) {
          rows.add(row);
          if (rows.size() >= K) {
            return rows;
          }
        }
      }
      if (rows.size() >= K) {
        break;
      }
    }
    return rows;
  }

  private static boolean sameTopKKeys(List<Q10TopKRow> left, List<Q10TopKRow> right) {
    if (left.size() != right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      if (left.get(i).cCustkey() != right.get(i).cCustkey()
          || left.get(i).revenueCents() != right.get(i).revenueCents()) {
        return false;
      }
    }
    return true;
  }
}
