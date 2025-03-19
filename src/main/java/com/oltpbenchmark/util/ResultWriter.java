/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.util;

import com.oltpbenchmark.DistributionStatistics;
import com.oltpbenchmark.LatencyRecord;
import com.oltpbenchmark.Results;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.collectors.DBParameterCollector;
import com.oltpbenchmark.api.collectors.DBParameterCollectorGen;
import com.oltpbenchmark.types.DatabaseType;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultWriter {

  private static final Logger LOG = LoggerFactory.getLogger(ResultWriter.class);

  public static final double MILLISECONDS_FACTOR = 1e3;

  private static final String[] IGNORE_CONF = {"type", "driver", "url", "username", "password"};

  private static final String[] BENCHMARK_KEY_FIELD = {"isolation", "scalefactor", "terminals"};

  private final XMLConfiguration expConf;
  private final DBParameterCollector collector;
  private final Results results;
  private final DatabaseType dbType;
  private final String benchType;

  public ResultWriter(Results r, XMLConfiguration conf, CommandLine argsLine) {
    this.expConf = conf;
    this.results = r;
    this.dbType = DatabaseType.valueOf(expConf.getString("type").toUpperCase());
    this.benchType = argsLine.getOptionValue("b");

    String dbUrl = expConf.getString("url");
    String username = expConf.getString("username");
    String password = expConf.getString("password");

    this.collector = DBParameterCollectorGen.getCollector(dbType, dbUrl, username, password);
  }

  public void writeParams(PrintStream os) {
    String dbConf = collector.collectParameters();
    os.print(dbConf);
  }

  public void writeMetrics(PrintStream os) {
    os.print(collector.collectMetrics());
  }

  public boolean hasMetrics() {
    return collector.hasMetrics();
  }

  public void writeConfig(PrintStream os) throws ConfigurationException {

    XMLConfiguration outputConf = (XMLConfiguration) expConf.clone();
    for (String key : IGNORE_CONF) {
      outputConf.clearProperty(key);
    }

    FileHandler handler = new FileHandler(outputConf);
    handler.save(os);
  }

  public void writeSummary(PrintStream os) {
    Map<String, Object> summaryMap = new LinkedHashMap<>();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    Date now = new Date();
    long startTimestampMs = results.getStartTimestampMs();
    long currentTimestampMs = now.getTime();
    long totalDurationMs = currentTimestampMs - startTimestampMs;

    summaryMap.put("Start Timestamp (milliseconds)", startTimestampMs);
    summaryMap.put("Current Timestamp (milliseconds)", currentTimestampMs);
    summaryMap.put("Total Duration (milliseconds)", totalDurationMs);
    summaryMap.put("Elapsed Time (nanoseconds)", results.getNanoseconds());

    // Calculate the duration for each measurement window
    Collection<Results.MeasurementWindow> windows = Results.getMeasurementWindows();
    if (!windows.isEmpty()) {
      long paramWindows = 0;
      long windowsDurationMs = 0;

      List<Map<String, Object>> windowsInfo = new ArrayList<>();
      for (Results.MeasurementWindow window : windows) {
        long windowDuration = window.getEndTimeMs() - window.getStartTimeMs();
        windowsDurationMs += windowDuration;
        paramWindows++;

        Map<String, Object> windowInfo = new LinkedHashMap<>();
        windowInfo.put("Parameter Set", window.getParameterSetId());
        windowInfo.put("Start Time (ms)", window.getStartTimeMs());
        windowInfo.put("End Time (ms)", window.getEndTimeMs());
        windowInfo.put("Duration (ms)", windowDuration);
        windowsInfo.add(windowInfo);
      }

      summaryMap.put("Parameter Measurement Windows", paramWindows);
      summaryMap.put("Total Parameter Windows Duration (ms)", windowsDurationMs);

      // Add percent of time spent in windows vs total
      double windowsPercentage = (double) windowsDurationMs / (double) totalDurationMs * 100.0;
      summaryMap.put(
          "Parameter Windows Time Percentage", String.format("%.2f%%", windowsPercentage));

      // Add detailed window information
      summaryMap.put("Windows", windowsInfo);
    }

    summaryMap.put("DBMS Type", dbType);
    summaryMap.put("DBMS Version", collector.collectVersion());
    summaryMap.put("Benchmark Type", benchType);
    summaryMap.put("Final State", results.getState());
    summaryMap.put("Measured Requests", results.getMeasuredRequests());
    for (String field : BENCHMARK_KEY_FIELD) {
      summaryMap.put(field, expConf.getString(field));
    }
    summaryMap.put("Latency Distribution", results.getDistributionStatistics().toMap());
    summaryMap.put("Throughput (requests/second)", results.requestsPerSecondThroughput());
    summaryMap.put("Goodput (requests/second)", results.requestsPerSecondGoodput());
    os.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
  }

  public void writeResults(int windowSizeSeconds, PrintStream out) {
    writeResults(windowSizeSeconds, out, TransactionType.INVALID);
  }

  public void writeResults(int windowSizeSeconds, PrintStream out, TransactionType txType) {
    String[] header = {
      "Time (seconds)",
      "Throughput (requests/second)",
      "Average Latency (millisecond)",
      "Minimum Latency (millisecond)",
      "25th Percentile Latency (millisecond)",
      "Median Latency (millisecond)",
      "75th Percentile Latency (millisecond)",
      "90th Percentile Latency (millisecond)",
      "95th Percentile Latency (millisecond)",
      "99th Percentile Latency (millisecond)",
      "Maximum Latency (millisecond)",
      "tp (req/s) scaled"
    };
    out.println(StringUtil.join(",", header));
    int i = 0;
    for (DistributionStatistics s :
        new ThreadBench.TimeBucketIterable(
            results.getLatencySamples(), windowSizeSeconds, txType)) {
      out.printf(
          "%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
          i * windowSizeSeconds,
          (double) s.getCount() / windowSizeSeconds,
          s.getAverage() / MILLISECONDS_FACTOR,
          s.getMinimum() / MILLISECONDS_FACTOR,
          s.get25thPercentile() / MILLISECONDS_FACTOR,
          s.getMedian() / MILLISECONDS_FACTOR,
          s.get75thPercentile() / MILLISECONDS_FACTOR,
          s.get90thPercentile() / MILLISECONDS_FACTOR,
          s.get95thPercentile() / MILLISECONDS_FACTOR,
          s.get99thPercentile() / MILLISECONDS_FACTOR,
          s.getMaximum() / MILLISECONDS_FACTOR,
          MILLISECONDS_FACTOR / s.getAverage());
      i += 1;
    }
  }

  public void writeSamples(PrintStream out) {
    writeSamples(1, out, TransactionType.INVALID);
  }

  public void writeSamples(int windowSizeSeconds, PrintStream out, TransactionType txType) {
    String[] header = {
      "Time (seconds)",
      "Requests",
      "Throughput (requests/second)",
      "Minimum Latency (microseconds)",
      "25th Percentile Latency (microseconds)",
      "Median Latency (microseconds)",
      "Average Latency (microseconds)",
      "75th Percentile Latency (microseconds)",
      "90th Percentile Latency (microseconds)",
      "95th Percentile Latency (microseconds)",
      "99th Percentile Latency (microseconds)",
      "Maximum Latency (microseconds)"
    };
    out.println(StringUtil.join(",", header));
    int i = 0;
    for (DistributionStatistics s :
        new ThreadBench.TimeBucketIterable(
            results.getLatencySamples(), windowSizeSeconds, txType)) {
      out.printf(
          "%d,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
          i * windowSizeSeconds,
          s.getCount(),
          (double) s.getCount() / windowSizeSeconds,
          (int) s.getMinimum(),
          (int) s.get25thPercentile(),
          (int) s.getMedian(),
          (int) s.getAverage(),
          (int) s.get75thPercentile(),
          (int) s.get90thPercentile(),
          (int) s.get95thPercentile(),
          (int) s.get99thPercentile(),
          (int) s.getMaximum());
      i += 1;
    }
  }

  public void writeRaw(List<TransactionType> activeTXTypes, PrintStream out) {

    // This is needed because nanTime does not guarantee offset... we
    // ground it (and round it) to ms from 1970-01-01 like currentTime
    double x = ((double) System.nanoTime() / (double) 1000000000);
    double y = ((double) System.currentTimeMillis() / (double) 1000);
    double offset = x - y;

    // long startNs = latencySamples.get(0).startNs;
    String[] header = {
      "Transaction Type Index",
      "Transaction Name",
      "Start Time (microseconds)",
      "Latency (microseconds)",
      "Worker Id (start number)",
      "Phase Id (index in config file)"
    };
    out.println(StringUtil.join(",", header));
    for (LatencyRecord.Sample s : results.getLatencySamples()) {
      double startUs = ((double) s.getStartNanosecond() / (double) 1000000000);
      String[] row = {
        Integer.toString(s.getTransactionType()),
        // Important!
        // The TxnType offsets start at 1!
        activeTXTypes.get(s.getTransactionType() - 1).getName(),
        String.format("%10.6f", startUs - offset),
        Integer.toString(s.getLatencyMicrosecond()),
        Integer.toString(s.getWorkerId()),
        Integer.toString(s.getPhaseId()),
      };
      out.println(StringUtil.join(",", row));
    }
  }

  /**
   * Write summary histograms for each scheduler parameter measurement window. This generates one
   * summary file per measurement window.
   *
   * @param activeTXTypes List of active transaction types
   * @param outputDirectory Directory to write output files
   * @param baseFileName Base file name for output files
   */
  public void writeParameterWindowHistograms(
      List<TransactionType> activeTXTypes, String outputDirectory, String baseFileName) {

    // Get all measurement windows
    Collection<Results.MeasurementWindow> windows = Results.getMeasurementWindows();
    if (windows.isEmpty()) {
      LOG.info("No parameter measurement windows found, skipping window histograms");
      return;
    }

    LOG.info("Generating histograms for {} parameter measurement windows", windows.size());

    // Get time offset to convert nanoseconds to milliseconds since epoch
    double x = ((double) System.nanoTime() / (double) 1000000000);
    double y = ((double) System.currentTimeMillis() / (double) 1000);
    double offset = x - y;

    // First run verification to check for overlapping windows or samples
    verifyNoOverlaps(windows, offset);

    // Create a list to hold "trimming" samples (those not in any window)
    List<LatencyRecord.Sample> trimmingSamples = new ArrayList<>();

    // First pass - identify trimmings (samples not in any window)
    for (LatencyRecord.Sample sample : results.getLatencySamples()) {
      double startTimeSeconds = ((double) sample.getStartNanosecond() / 1_000_000_000.0);
      long sampleTimeMs = (long) ((startTimeSeconds - offset) * 1000.0);

      boolean belongsToWindow = false;
      for (Results.MeasurementWindow window : windows) {
        if (window.containsTimestamp(sampleTimeMs)) {
          belongsToWindow = true;
          break;
        }
      }

      if (!belongsToWindow) {
        trimmingSamples.add(sample);
      }
    }

    LOG.info(
        "Found {} samples not belonging to any measurement window (trimmings)",
        trimmingSamples.size());

    // Create a trimmings histogram and summary file
    if (!trimmingSamples.isEmpty()) {
      writeTrimminsSummary(activeTXTypes, trimmingSamples, outputDirectory, baseFileName, offset);
    }

    // For each window, generate a summary with transactions only from that window
    for (Results.MeasurementWindow window : windows) {
      // Filter samples that fall within this window's time range
      List<LatencyRecord.Sample> windowSamples = new ArrayList<>();
      for (LatencyRecord.Sample sample : results.getLatencySamples()) {
        double startTimeSeconds = ((double) sample.getStartNanosecond() / 1_000_000_000.0);
        long sampleTimeMs = (long) ((startTimeSeconds - offset) * 1000.0);

        if (window.containsTimestamp(sampleTimeMs)) {
          windowSamples.add(sample);
        }
      }

      // Skip if no samples in this window
      if (windowSamples.isEmpty()) {
        LOG.info(
            "No samples found for window {} ({} - {} ms), skipping",
            window.getParameterSetId(),
            window.getStartTimeMs(),
            window.getEndTimeMs());
        continue;
      }

      // Write raw samples to a CSV file for this window
      writeWindowSamplesCSV(
          activeTXTypes, windowSamples, window, outputDirectory, baseFileName, offset);

      // Compute statistics for this window
      int[] latencies = new int[windowSamples.size()];
      for (int i = 0; i < windowSamples.size(); ++i) {
        latencies[i] = windowSamples.get(i).getLatencyMicrosecond();
      }
      DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

      // Create histograms
      Histogram<TransactionType> windowSuccess = new Histogram<>(true);
      Histogram<TransactionType> windowAbort = new Histogram<>(false);
      Histogram<TransactionType> windowRetry = new Histogram<>(false);
      Histogram<TransactionType> windowError = new Histogram<>(false);
      Histogram<TransactionType> windowUnknown = new Histogram<>(false);

      // Count transactions directly from samples
      int successCount = 0;
      int abortCount = 0;
      int retryCount = 0;
      int errorCount = 0;
      int unknownCount = 0;

      // Create maps to store transaction counts by type
      Map<TransactionType, Integer> txTypeToCount = new HashMap<>();

      // First, count all transaction types in this window
      for (LatencyRecord.Sample sample : windowSamples) {
        if (sample.getTransactionType() <= 0
            || sample.getTransactionType() > activeTXTypes.size()) {
          continue; // Skip invalid transaction types
        }

        // Get the transaction type for this sample
        TransactionType txType = activeTXTypes.get(sample.getTransactionType() - 1);

        // Increment the count for this transaction type
        txTypeToCount.put(txType, txTypeToCount.getOrDefault(txType, 0) + 1);
      }

      // Now populate the histograms based on the transaction status
      // We'll use the global histograms only to decide the default status for a transaction type
      for (Map.Entry<TransactionType, Integer> entry : txTypeToCount.entrySet()) {
        TransactionType txType = entry.getKey();
        int count = entry.getValue();

        // Check if this transaction type has been marked with a status
        if (results.getSuccess().get(txType, 0) > 0) {
          windowSuccess.put(txType, count);
          successCount += count;
        } else if (results.getAbort().get(txType, 0) > 0) {
          windowAbort.put(txType, count);
          abortCount += count;
        } else if (results.getRetry().get(txType, 0) > 0
            || results.getRetryDifferent().get(txType, 0) > 0) {
          windowRetry.put(txType, count);
          retryCount += count;
        } else if (results.getError().get(txType, 0) > 0) {
          windowError.put(txType, count);
          errorCount += count;
        } else {
          windowUnknown.put(txType, count);
          unknownCount += count;
        }
      }

      // Create a summary file for this window
      String paramStr = window.getParameterSetId();
      String summaryFileName = baseFileName + ".window." + paramStr + ".summary.json";
      Map<String, Object> summaryMap = new LinkedHashMap<>();

      // Add window metadata
      summaryMap.put("Parameter Set", window.getParameterSetId());
      summaryMap.put("Parameters", window.getParameters());
      summaryMap.put("Window Start Time (ms)", window.getStartTimeMs());
      summaryMap.put("Window End Time (ms)", window.getEndTimeMs());
      summaryMap.put("Window Duration (ms)", window.getEndTimeMs() - window.getStartTimeMs());
      summaryMap.put("Samples in Window", windowSamples.size());

      // Log a warning if there's still a discrepancy
      int totalOutcomeCount = successCount + abortCount + retryCount + errorCount + unknownCount;
      if (totalOutcomeCount != windowSamples.size()) {
        LOG.warn(
            "Transaction outcome count ({}) doesn't match sample count ({}) for window {}",
            totalOutcomeCount,
            windowSamples.size(),
            window.getParameterSetId());
      }

      // Add standard summary data
      summaryMap.put("DBMS Type", dbType);
      summaryMap.put("DBMS Version", collector.collectVersion());
      summaryMap.put("Benchmark Type", benchType);

      // Add window-specific statistics
      summaryMap.put("Latency Distribution", stats.toMap());

      // Calculate throughput for this window
      double windowDurationSeconds = (window.getEndTimeMs() - window.getStartTimeMs()) / 1000.0;
      if (windowDurationSeconds > 0) {
        double throughput = windowSamples.size() / windowDurationSeconds;
        summaryMap.put("Throughput (requests/second)", throughput);

        // Calculate goodput (successful transactions per second)
        double goodput = successCount / windowDurationSeconds;
        summaryMap.put("Goodput (requests/second)", goodput);
      }

      // Add transaction outcome totals for easier reference
      Map<String, Integer> outcomeTotals = new LinkedHashMap<>();
      outcomeTotals.put("Success", successCount);
      outcomeTotals.put("Abort", abortCount);
      outcomeTotals.put("Retry", retryCount);
      outcomeTotals.put("Error", errorCount);
      outcomeTotals.put("Unknown", unknownCount);
      outcomeTotals.put("Total", windowSamples.size());
      summaryMap.put("Transaction Outcomes", outcomeTotals);

      // Add transaction histograms
      Map<String, Object> histograms = new LinkedHashMap<>();
      histograms.put("success", windowSuccess);
      histograms.put("abort", windowAbort);
      histograms.put("retry", windowRetry);
      histograms.put("error", windowError);
      histograms.put("unknown", windowUnknown);
      summaryMap.put("histograms", histograms);

      // Write the summary to a file
      try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, summaryFileName))) {
        ps.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
        LOG.info("Wrote window histogram summary to {}", summaryFileName);
      } catch (Exception e) {
        LOG.error("Error writing window histogram summary: {}", e.getMessage());
      }
    }
  }

  /**
   * Verify that there are no overlaps between measurement windows and that each sample is only
   * assigned to one window or to trimmings.
   *
   * @param windows Collection of measurement windows to verify
   * @param offset Time offset for timestamp conversion
   */
  private void verifyNoOverlaps(Collection<Results.MeasurementWindow> windows, double offset) {
    // Check for overlapping windows
    List<Results.MeasurementWindow> windowList = new ArrayList<>(windows);
    windowList.sort(Comparator.comparingLong(Results.MeasurementWindow::getStartTimeMs));

    // Calculate total benchmark duration
    long totalDurationMs = 0;

    // Check for overlapping windows
    for (int i = 0; i < windowList.size() - 1; i++) {
      Results.MeasurementWindow current = windowList.get(i);
      Results.MeasurementWindow next = windowList.get(i + 1);

      long windowDuration = current.getEndTimeMs() - current.getStartTimeMs();
      totalDurationMs += windowDuration;

      LOG.info("Window {} duration: {}ms", current.getParameterSetId(), windowDuration);

      if (current.getEndTimeMs() > next.getStartTimeMs()) {
        LOG.warn(
            "OVERLAP DETECTED: Window {} (end: {}) overlaps with Window {} (start: {})",
            current.getParameterSetId(),
            current.getEndTimeMs(),
            next.getParameterSetId(),
            next.getStartTimeMs());
      }
    }

    // Add duration of the last window
    if (!windowList.isEmpty()) {
      Results.MeasurementWindow lastWindow = windowList.get(windowList.size() - 1);
      long lastWindowDuration = lastWindow.getEndTimeMs() - lastWindow.getStartTimeMs();
      totalDurationMs += lastWindowDuration;
      LOG.info("Window {} duration: {}ms", lastWindow.getParameterSetId(), lastWindowDuration);
    }

    LOG.info("Total windows duration: {}ms", totalDurationMs);

    // Check for samples that belong to multiple windows
    Map<LatencyRecord.Sample, List<String>> sampleWindows = new HashMap<>();

    for (Results.MeasurementWindow window : windows) {
      for (LatencyRecord.Sample sample : results.getLatencySamples()) {
        double startTimeSeconds = ((double) sample.getStartNanosecond() / 1_000_000_000.0);
        long sampleTimeMs = (long) ((startTimeSeconds - offset) * 1000.0);

        if (window.containsTimestamp(sampleTimeMs)) {
          sampleWindows
              .computeIfAbsent(sample, k -> new ArrayList<>())
              .add(window.getParameterSetId());
        }
      }
    }

    // Check if any sample belongs to multiple windows
    int multiWindowSamples = 0;
    for (Map.Entry<LatencyRecord.Sample, List<String>> entry : sampleWindows.entrySet()) {
      if (entry.getValue().size() > 1) {
        multiWindowSamples++;
        if (multiWindowSamples <= 5) { // Limit detailed logging to first 5 samples
          LOG.warn(
              "Sample {} appears in multiple windows: {}",
              entry.getKey().getTransactionType(),
              String.join(", ", entry.getValue()));
        }
      }
    }

    if (multiWindowSamples > 0) {
      LOG.warn(
          "OVERLAP DETECTED: {} samples appear in multiple measurement windows",
          multiWindowSamples);
    } else {
      LOG.info("Verification complete: No overlapping samples detected");
    }
  }

  /**
   * Write the "trimmings" samples (those not in any measurement window) to a summary file
   *
   * @param activeTXTypes List of active transaction types
   * @param trimmingSamples List of samples not in any window
   * @param outputDirectory Directory to write output files
   * @param baseFileName Base file name for output files
   * @param offset Time offset for calculation
   */
  private void writeTrimminsSummary(
      List<TransactionType> activeTXTypes,
      List<LatencyRecord.Sample> trimmingSamples,
      String outputDirectory,
      String baseFileName,
      double offset) {

    // Write raw samples to CSV file for trimmings
    String trimmingsCSVFileName = baseFileName + ".trimmings.csv";
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(outputDirectory, trimmingsCSVFileName))) {
      // Write CSV header
      String[] header = {
        "Transaction Type",
        "Transaction Name",
        "Start Time (ms)",
        "Latency (microseconds)",
        "Worker Id",
        "Phase Id"
      };
      ps.println(StringUtil.join(",", header));

      // Write each sample
      for (LatencyRecord.Sample sample : trimmingSamples) {
        double startTimeSeconds = ((double) sample.getStartNanosecond() / 1_000_000_000.0);
        long startTimeMs = (long) ((startTimeSeconds - offset) * 1000.0);

        String txnName = "UNKNOWN";
        if (sample.getTransactionType() > 0
            && sample.getTransactionType() <= activeTXTypes.size()) {
          txnName = activeTXTypes.get(sample.getTransactionType() - 1).getName();
        }

        ps.printf(
            "%d,%s,%d,%d,%d,%d\n",
            sample.getTransactionType(),
            txnName,
            startTimeMs,
            sample.getLatencyMicrosecond(),
            sample.getWorkerId(),
            sample.getPhaseId());
      }

      LOG.info("Wrote {} trimming samples to {}", trimmingSamples.size(), trimmingsCSVFileName);
    } catch (Exception e) {
      LOG.error("Error writing trimming samples to CSV file: {}", e.getMessage());
    }

    // Create histogram for trimmings
    if (trimmingSamples.isEmpty()) {
      return;
    }

    // Compute statistics
    int[] latencies = new int[trimmingSamples.size()];
    for (int i = 0; i < trimmingSamples.size(); ++i) {
      latencies[i] = trimmingSamples.get(i).getLatencyMicrosecond();
    }
    DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

    // Create histograms for each outcome category
    Histogram<TransactionType> trimmingsSuccessHistogram = new Histogram<>(true);
    Histogram<TransactionType> trimmingsAbortHistogram = new Histogram<>(false);
    Histogram<TransactionType> trimmingsRetryHistogram = new Histogram<>(false);
    Histogram<TransactionType> trimmingsErrorHistogram = new Histogram<>(false);
    Histogram<TransactionType> trimmingsUnknownHistogram = new Histogram<>(false);

    // Count by outcome categories
    int successCount = 0;
    int abortCount = 0;
    int retryCount = 0;
    int errorCount = 0;
    int unknownCount = 0;

    // Create maps to store transaction counts by type
    Map<TransactionType, Integer> txTypeToCount = new HashMap<>();

    // First, count all transaction types in trimmings
    for (LatencyRecord.Sample sample : trimmingSamples) {
      if (sample.getTransactionType() > 0 && sample.getTransactionType() <= activeTXTypes.size()) {
        TransactionType txType = activeTXTypes.get(sample.getTransactionType() - 1);

        // Increment the count for this transaction type
        txTypeToCount.put(txType, txTypeToCount.getOrDefault(txType, 0) + 1);
      }
    }

    // Now populate the histograms based on the transaction status
    // We'll use the global histograms only to decide the default status for a transaction type
    for (Map.Entry<TransactionType, Integer> entry : txTypeToCount.entrySet()) {
      TransactionType txType = entry.getKey();
      int count = entry.getValue();

      // Check if this transaction type has been marked with a status
      if (results.getSuccess().get(txType, 0) > 0) {
        trimmingsSuccessHistogram.put(txType, count);
        successCount += count;
      } else if (results.getAbort().get(txType, 0) > 0) {
        trimmingsAbortHistogram.put(txType, count);
        abortCount += count;
      } else if (results.getRetry().get(txType, 0) > 0
          || results.getRetryDifferent().get(txType, 0) > 0) {
        trimmingsRetryHistogram.put(txType, count);
        retryCount += count;
      } else if (results.getError().get(txType, 0) > 0) {
        trimmingsErrorHistogram.put(txType, count);
        errorCount += count;
      } else {
        trimmingsUnknownHistogram.put(txType, count);
        unknownCount += count;
      }
    }

    // Create summary file for trimmings
    String trimmingsSummaryFileName = baseFileName + ".trimmings.summary.json";
    Map<String, Object> summaryMap = new LinkedHashMap<>();

    // Add metadata
    summaryMap.put("Type", "Trimmings");
    summaryMap.put(
        "Description", "Transactions not belonging to any scheduler parameter measurement window");
    summaryMap.put("Samples Count", trimmingSamples.size());

    // Calculate start and end times for trimmings
    if (!trimmingSamples.isEmpty()) {
      long minStartTime = Long.MAX_VALUE;
      long maxEndTime = Long.MIN_VALUE;

      for (LatencyRecord.Sample sample : trimmingSamples) {
        double startTimeSeconds = ((double) sample.getStartNanosecond() / 1_000_000_000.0);
        long sampleTimeMs = (long) ((startTimeSeconds - offset) * 1000.0);
        long endTimeMs = sampleTimeMs + (sample.getLatencyMicrosecond() / 1000);

        minStartTime = Math.min(minStartTime, sampleTimeMs);
        maxEndTime = Math.max(maxEndTime, endTimeMs);
      }

      if (minStartTime < Long.MAX_VALUE && maxEndTime > Long.MIN_VALUE) {
        summaryMap.put("First Transaction Time (ms)", minStartTime);
        summaryMap.put("Last Transaction End Time (ms)", maxEndTime);
        summaryMap.put("Time Span (ms)", maxEndTime - minStartTime);

        // Calculate throughput and goodput metrics
        double timeSpanSeconds = (maxEndTime - minStartTime) / 1000.0;
        if (timeSpanSeconds > 0) {
          // Calculate throughput (all transactions per second)
          double throughput = trimmingSamples.size() / timeSpanSeconds;
          summaryMap.put("Throughput (requests/second)", throughput);

          // Calculate goodput (successful transactions per second)
          double goodput = successCount / timeSpanSeconds;
          summaryMap.put("Goodput (requests/second)", goodput);
        }
      }
    }

    // Add standard summary data
    summaryMap.put("DBMS Type", dbType);
    summaryMap.put("DBMS Version", collector.collectVersion());
    summaryMap.put("Benchmark Type", benchType);

    // Add statistics
    summaryMap.put("Latency Distribution", stats.toMap());

    // Add transaction outcome totals for easier reference
    Map<String, Integer> outcomeTotals = new LinkedHashMap<>();
    outcomeTotals.put("Success", successCount);
    outcomeTotals.put("Abort", abortCount);
    outcomeTotals.put("Retry", retryCount);
    outcomeTotals.put("Error", errorCount);
    outcomeTotals.put("Unknown", unknownCount);
    outcomeTotals.put("Total", trimmingSamples.size());
    summaryMap.put("Transaction Outcomes", outcomeTotals);

    // Add all histograms
    Map<String, Object> histograms = new LinkedHashMap<>();
    histograms.put("success", trimmingsSuccessHistogram);
    histograms.put("abort", trimmingsAbortHistogram);
    histograms.put("retry", trimmingsRetryHistogram);
    histograms.put("error", trimmingsErrorHistogram);
    histograms.put("unknown", trimmingsUnknownHistogram);
    summaryMap.put("histograms", histograms);

    // Write the summary to a file
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(outputDirectory, trimmingsSummaryFileName))) {
      ps.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
      LOG.info("Wrote trimmings summary to {}", trimmingsSummaryFileName);
    } catch (Exception e) {
      LOG.error("Error writing trimmings summary: {}", e.getMessage());
    }
  }

  /**
   * Write raw samples for a specific window to a CSV file
   *
   * @param activeTXTypes List of active transaction types
   * @param windowSamples List of samples in this window
   * @param window The measurement window
   * @param outputDirectory Directory to write output files
   * @param baseFileName Base file name for output files
   * @param offset Time offset for calculation
   */
  private void writeWindowSamplesCSV(
      List<TransactionType> activeTXTypes,
      List<LatencyRecord.Sample> windowSamples,
      Results.MeasurementWindow window,
      String outputDirectory,
      String baseFileName,
      double offset) {

    String windowCSVFileName =
        baseFileName + ".window." + window.getParameterSetId() + ".samples.csv";
    try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, windowCSVFileName))) {
      // Write CSV header
      String[] header = {
        "Transaction Type",
        "Transaction Name",
        "Start Time (ms)",
        "Latency (microseconds)",
        "Worker Id",
        "Phase Id"
      };
      ps.println(StringUtil.join(",", header));

      // Write each sample
      for (LatencyRecord.Sample sample : windowSamples) {
        double startTimeSeconds = ((double) sample.getStartNanosecond() / 1_000_000_000.0);
        long startTimeMs = (long) ((startTimeSeconds - offset) * 1000.0);

        String txnName = "UNKNOWN";
        if (sample.getTransactionType() > 0
            && sample.getTransactionType() <= activeTXTypes.size()) {
          txnName = activeTXTypes.get(sample.getTransactionType() - 1).getName();
        }

        ps.printf(
            "%d,%s,%d,%d,%d,%d\n",
            sample.getTransactionType(),
            txnName,
            startTimeMs,
            sample.getLatencyMicrosecond(),
            sample.getWorkerId(),
            sample.getPhaseId());
      }

      LOG.info(
          "Wrote {} samples to {} for window {}",
          windowSamples.size(),
          windowCSVFileName,
          window.getParameterSetId());
    } catch (Exception e) {
      LOG.error("Error writing window samples to CSV file: {}", e.getMessage());
    }
  }
}
