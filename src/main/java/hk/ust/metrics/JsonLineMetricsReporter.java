package hk.ust.metrics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Appends one JSON line per run to a file. */
public final class JsonLineMetricsReporter implements MetricsReporter {

  private final Path outputPath;

  public JsonLineMetricsReporter(Path outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public void report(MetricsSnapshot snapshot) {
    try {
      Path parent = outputPath.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      Files.writeString(
          outputPath,
          snapshot.toJsonLine() + System.lineSeparator(),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
      System.out.println("L0 metrics written to: " + outputPath.toAbsolutePath());
    } catch (IOException e) {
      System.err.println("Failed to write L0 metrics: " + e.getMessage());
    }
  }
}
