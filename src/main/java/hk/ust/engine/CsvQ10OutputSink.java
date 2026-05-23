package hk.ust.engine;

import hk.ust.aggregate.Q10TopKWriter;
import hk.ust.aggregate.Q10RevenueAggregator;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Writes Q10 top-K snapshots; on {@link OutputTrigger#JOB_END} writes the final CSV. */
public final class CsvQ10OutputSink implements Q10OutputSink {

  private final Path outputPath;
  private long snapshotCount;

  public CsvQ10OutputSink(String outputPath) {
    this.outputPath = Path.of(outputPath);
  }

  @Override
  public void emitTopK(List<Q10TopKRow> rows, OutputTrigger trigger) throws Exception {
    snapshotCount++;
    if (trigger == OutputTrigger.JOB_END) {
      writeJobEndCsv(rows);
      return;
    }

    Path snapshotPath =
        outputPath.resolveSibling(
            outputPath.getFileName()
                + "."
                + trigger.name().toLowerCase()
                + "."
                + snapshotCount
                + ".csv");
    writeRows(snapshotPath, rows);
    System.out.printf(
        "Q10 top-K snapshot #%d (%s) written to: %s%n",
        snapshotCount, trigger, snapshotPath.toAbsolutePath());
  }

  private void writeJobEndCsv(List<Q10TopKRow> rows) throws Exception {
    writeRows(outputPath, rows);
    System.out.println("Q10 top-K results written to: " + outputPath.toAbsolutePath());
  }

  private static void writeRows(Path path, List<Q10TopKRow> rows) throws Exception {
    Files.createDirectories(path.getParent());
    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
      pw.println("c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment");
      for (Q10TopKRow row : rows) {
        pw.printf(
            "%d,%s,%.4f,%.2f,%s,\"%s\",%s,\"%s\"%n",
            row.cCustkey(),
            row.cName(),
            row.revenueCents() / 100.0,
            row.cAcctbalCents() / 100.0,
            row.nName(),
            row.cAddress().replace("\"", "\"\""),
            row.cPhone(),
            row.cComment().replace("\"", "\"\""));
      }
    }
  }

  public static List<Q10TopKRow> rowsFromAggregator(Q10RevenueAggregator aggregator) {
    return Q10TopKWriter.buildTopKRows(aggregator);
  }
}
