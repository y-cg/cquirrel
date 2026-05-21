package hk.ust.source;

import hk.ust.model.TupleUpdate;
import hk.ust.model.TupleUpdate.*;
import hk.ust.model.UpdateType;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;

/** Counts tuple updates in TPC-H data directory (same layout as {@link TpchUpdateSource}). */
public final class TpchUpdateCounter {

  private TpchUpdateCounter() {}

  public static long count(Path dir) throws Exception {
    long total = 0;
    total += countTable(dir.resolve("nation.tbl"), "nation");
    total += countTable(dir.resolve("customer.tbl"), "customer");
    total += countTable(dir.resolve("orders.tbl"), "orders");
    total += countTable(dir.resolve("lineitem.tbl"), "lineitem");

    for (int i = 1; ; i++) {
      Path lineitemUpdate = dir.resolve("lineitem.tbl.u" + i);
      Path ordersUpdate = dir.resolve("orders.tbl.u" + i);
      if (!Files.exists(lineitemUpdate) && !Files.exists(ordersUpdate)) {
        break;
      }
      if (Files.exists(ordersUpdate)) {
        total += countUpdateFile(ordersUpdate, "orders");
      }
      if (Files.exists(lineitemUpdate)) {
        total += countUpdateFile(lineitemUpdate, "lineitem");
      }
    }
    return total;
  }

  private static long countTable(Path file, String relation) throws Exception {
    if (!Files.exists(file)) {
      return 0;
    }
    long count = 0;
    try (BufferedReader reader = Files.newBufferedReader(file)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }
        if (parseLine(line, relation, UpdateType.INSERT) != null) {
          count++;
        }
      }
    }
    return count;
  }

  private static long countUpdateFile(Path file, String relation) throws Exception {
    if (!Files.exists(file)) {
      return 0;
    }
    long count = 0;
    try (BufferedReader reader = Files.newBufferedReader(file)) {
      String line;
      boolean isDelete = true;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }
        UpdateType type = isDelete ? UpdateType.DELETE : UpdateType.INSERT;
        if (parseLine(line, relation, type) != null) {
          count++;
        }
        isDelete = !isDelete;
      }
    }
    return count;
  }

  private static TupleUpdate parseLine(String line, String relation, UpdateType type) {
    return switch (relation) {
      case "nation" -> new NationUpdate(type, TpchCsvParser.parseNation(line));
      case "customer" -> new CustomerUpdate(type, TpchCsvParser.parseCustomer(line));
      case "orders" -> new OrdersUpdate(type, TpchCsvParser.parseOrders(line));
      case "lineitem" -> new LineitemUpdate(type, TpchCsvParser.parseLineitem(line));
      default -> null;
    };
  }
}
