package hk.ust.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/** Samples JVM heap usage and tracks the peak during a run. */
final class MemorySampler {

  private final MemoryMXBean heapBean = ManagementFactory.getMemoryMXBean();
  private long peakUsedBytes;

  void sample() {
    long used = heapBean.getHeapMemoryUsage().getUsed();
    if (used > peakUsedBytes) {
      peakUsedBytes = used;
    }
  }

  long peakUsedBytes() {
    return peakUsedBytes;
  }

  long maxBytes() {
    return heapBean.getHeapMemoryUsage().getMax();
  }

  static long toMb(long bytes) {
    return bytes <= 0 ? 0 : bytes / (1024 * 1024);
  }
}
