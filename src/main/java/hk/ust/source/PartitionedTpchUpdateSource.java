package hk.ust.source;

import hk.ust.metrics.FlinkOperatorTimings;
import hk.ust.metrics.Phase;
import hk.ust.model.*;
import hk.ust.model.TupleUpdate.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;

/**
 * TPC-H source for partitioned AJU pipelines.
 *
 * <p>Nation and customer bulk rows are bootstrapped inside {@link
 * hk.ust.aggregate.PartitionedQ10AjuFunction}. This source emits orders, lineitem, and incremental
 * update files only.
 */
public class PartitionedTpchUpdateSource extends RichSourceFunction<TupleUpdate> {

  private final String dataDir;
  private volatile boolean running = true;

  public PartitionedTpchUpdateSource(String dataDir) {
    this.dataDir = dataDir;
  }

  @Override
  public void run(SourceContext<TupleUpdate> ctx) throws Exception {
    Path dir = Path.of(dataDir);

    loadTable(dir.resolve("orders.tbl"), ctx, "orders");
    loadTable(dir.resolve("lineitem.tbl"), ctx, "lineitem");

    for (int i = 1; running; i++) {
      Path lineitemUpdate = dir.resolve("lineitem.tbl.u" + i);
      Path ordersUpdate = dir.resolve("orders.tbl.u" + i);
      if (!Files.exists(lineitemUpdate) && !Files.exists(ordersUpdate)) {
        break;
      }
      if (Files.exists(ordersUpdate)) {
        loadUpdateFile(ordersUpdate, ctx, "orders");
      }
      if (Files.exists(lineitemUpdate)) {
        loadUpdateFile(lineitemUpdate, ctx, "lineitem");
      }
    }
  }

  private void loadTable(Path file, SourceContext<TupleUpdate> ctx, String relation)
      throws IOException {
    if (!Files.exists(file)) {
      return;
    }
    try (BufferedReader reader = Files.newBufferedReader(file)) {
      String line;
      while (running && (line = readTimedLine(reader)) != null) {
        if (line.isBlank()) {
          continue;
        }
        parseAndCollect(line, relation, UpdateType.INSERT, ctx);
      }
    }
  }

  private void loadUpdateFile(Path file, SourceContext<TupleUpdate> ctx, String relation)
      throws IOException {
    if (!Files.exists(file)) {
      return;
    }
    try (BufferedReader reader = Files.newBufferedReader(file)) {
      String line;
      boolean isDelete = true;
      while (running && (line = readTimedLine(reader)) != null) {
        if (line.isBlank()) {
          continue;
        }
        UpdateType type = isDelete ? UpdateType.DELETE : UpdateType.INSERT;
        parseAndCollect(line, relation, type, ctx);
        isDelete = !isDelete;
      }
    }
  }

  private String readTimedLine(BufferedReader reader) throws IOException {
    long t0 = System.nanoTime();
    String line = reader.readLine();
    FlinkOperatorTimings.add(Phase.LOAD, System.nanoTime() - t0);
    return line;
  }

  private TupleUpdate parseAndCollect(
      String line, String relation, UpdateType type, SourceContext<TupleUpdate> ctx) {
    long t0 = System.nanoTime();
    TupleUpdate update = parseLine(line, relation, type);
    FlinkOperatorTimings.add(Phase.LOAD, System.nanoTime() - t0);
    if (update != null) {
      t0 = System.nanoTime();
      ctx.collect(update);
      FlinkOperatorTimings.add(Phase.RUNTIME, System.nanoTime() - t0);
    }
    return update;
  }

  private TupleUpdate parseLine(String line, String relation, UpdateType type) {
    return switch (relation) {
      case "orders" -> new OrdersUpdate(type, TpchCsvParser.parseOrders(line));
      case "lineitem" -> new LineitemUpdate(type, TpchCsvParser.parseLineitem(line));
      default -> null;
    };
  }

  @Override
  public void cancel() {
    running = false;
  }
}
