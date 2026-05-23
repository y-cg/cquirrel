package hk.ust.source;

import hk.ust.model.OrdersTuple;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Maps {@code o_orderkey} to {@code o_custkey} for routing lineitem updates. */
public final class OrderCustKeyIndex implements Serializable {

  private static final ConcurrentHashMap<String, OrderCustKeyIndex> CACHE =
      new ConcurrentHashMap<>();

  private final Map<Long, Long> orderToCust;

  private OrderCustKeyIndex(Map<Long, Long> orderToCust) {
    this.orderToCust = orderToCust;
  }

  public static OrderCustKeyIndex load(Path dataDir) throws IOException {
    String key = dataDir.toAbsolutePath().toString();
    OrderCustKeyIndex cached = CACHE.get(key);
    if (cached != null) {
      return cached;
    }
    OrderCustKeyIndex loaded = new OrderCustKeyIndex(readOrders(dataDir.resolve("orders.tbl")));
    CACHE.put(key, loaded);
    return loaded;
  }

  public long custKeyForOrder(long orderKey) {
    Long custKey = orderToCust.get(orderKey);
    if (custKey == null) {
      throw new IllegalStateException("No customer found for order key " + orderKey);
    }
    return custKey;
  }

  private static Map<Long, Long> readOrders(Path ordersFile) throws IOException {
    Map<Long, Long> index = new HashMap<>();
    if (!Files.exists(ordersFile)) {
      return index;
    }
    try (BufferedReader reader = Files.newBufferedReader(ordersFile)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }
        OrdersTuple tuple = TpchCsvParser.parseOrders(line);
        index.put(tuple.oOrderkey(), tuple.oCustkey());
      }
    }
    return index;
  }
}
