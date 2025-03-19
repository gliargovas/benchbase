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

package com.oltpbenchmark;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.Histogram;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Results {

  private static final Logger LOG = LoggerFactory.getLogger(Results.class);

  private final State state;
  private final long startTimestampMs;
  private final long nanoseconds;
  private final int measuredRequests;
  private final DistributionStatistics distributionStatistics;
  private final List<LatencyRecord.Sample> latencySamples;
  private final Histogram<TransactionType> unknown = new Histogram<>(false);
  private final Histogram<TransactionType> success = new Histogram<>(true);
  private final Histogram<TransactionType> abort = new Histogram<>(false);
  private final Histogram<TransactionType> retry = new Histogram<>(false);
  private final Histogram<TransactionType> error = new Histogram<>(false);
  private final Histogram<TransactionType> retryDifferent = new Histogram<>(false);
  private final Map<TransactionType, Histogram<String>> abortMessages = new HashMap<>();
  public String baseFileName;

  // Collection to store active perf processes for monitoring and cleanup
  private static final Collection<Process> activeProcesses = new ConcurrentLinkedQueue<>();
  private static ExecutorService executor;
  private static boolean shutdownRequested = false;

  private static Map<String, List<Object>> schedulerParamOptions = new HashMap<>();
  private static int workerCount = 4; // Default worker count
  private static Thread perfThread = null;
  private static int MEASUREMENT_WINDOW_SECONDS = 15; // Default window length

  // Track measurement windows for later histogram generation
  private static final Map<String, MeasurementWindow> measurementWindows = new HashMap<>();

  /** Class to represent a measurement window with start/end times and parameter info */
  public static class MeasurementWindow {
    private final String parameterSetId;
    private final long startTimeMs;
    private final long endTimeMs;
    private final Map<String, Object> parameters;

    public MeasurementWindow(
        String id, long startTimeMs, long endTimeMs, Map<String, Object> params) {
      this.parameterSetId = id;
      this.startTimeMs = startTimeMs;
      this.endTimeMs = endTimeMs;
      this.parameters = params;
    }

    public String getParameterSetId() {
      return parameterSetId;
    }

    public long getStartTimeMs() {
      return startTimeMs;
    }

    public long getEndTimeMs() {
      return endTimeMs;
    }

    public Map<String, Object> getParameters() {
      return parameters;
    }

    public boolean containsTimestamp(long timestampMs) {
      return timestampMs >= startTimeMs && timestampMs <= endTimeMs;
    }
  }

  /** Get all measurement windows recorded during the benchmark */
  public static Collection<MeasurementWindow> getMeasurementWindows() {
    return measurementWindows.values();
  }

  /** Check if a given time falls within any measurement window */
  public static MeasurementWindow findWindowForTimestamp(long timestampMs) {
    for (MeasurementWindow window : measurementWindows.values()) {
      if (window.containsTimestamp(timestampMs)) {
        return window;
      }
    }
    return null;
  }

  /**
   * Calculate total needed time for all parameter combinations
   *
   * @return Time in seconds needed for all measurements
   */
  public static int calculateTotalRequiredTime() {
    // Calculate how many parameter combinations we have
    int combinations = 1;
    if (!schedulerParamOptions.isEmpty()) {
      combinations = 0;
      List<SchedulerParams> paramCombinations =
          generateSchedulerCombinations(schedulerParamOptions, workerCount);
      combinations = paramCombinations.size();
    } else {
      // Default set has 6 combinations (3 min_granularity × 2 latency values)
      combinations = 6;
    }

    // Each combination needs MEASUREMENT_WINDOW_SECONDS + PARAM_APPLY_WAIT_SECONDS
    final int PARAM_APPLY_WAIT_SECONDS = 2;

    return combinations * (MEASUREMENT_WINDOW_SECONDS + PARAM_APPLY_WAIT_SECONDS);
  }

  /**
   * Check if the benchmark is configured to run long enough for all parameter combinations
   *
   * @param benchmarkTimeSeconds The configured benchmark run time in seconds
   * @return true if time is sufficient, false otherwise
   */
  public static boolean isTimeConfigurationSufficient(int benchmarkTimeSeconds) {
    int requiredTime = calculateTotalRequiredTime();
    LOG.info("Total required time for all parameter combinations: {} seconds", requiredTime);
    LOG.info("Benchmark time: {} seconds", benchmarkTimeSeconds);
    return benchmarkTimeSeconds >= requiredTime;
  }

  /**
   * Generate warning message if benchmark time is insufficient
   *
   * @param benchmarkTimeSeconds The configured benchmark run time
   * @return Warning message or null if configuration is sufficient
   */
  public static String getTimeConfigurationWarning(int benchmarkTimeSeconds) {
    LOG.info("Benchmark time: {} seconds", benchmarkTimeSeconds);
    int requiredTime = calculateTotalRequiredTime();
    LOG.info("Required time: {} seconds", requiredTime);
    if (benchmarkTimeSeconds < requiredTime) {
      return String.format(
          "WARNING: Benchmark configured runtime (%d seconds) is less than required for all scheduler parameter combinations (%d seconds). "
              + "Some parameter combinations may not be tested. Recommended setting: at least %d seconds.",
          benchmarkTimeSeconds, requiredTime, requiredTime);
    }
    return null;
  }

  /** A simple class to represent scheduler parameters */
  public static class SchedulerParams {
    private final Map<String, Object> params;

    public SchedulerParams(Map<String, Object> params) {
      this.params = params;
    }

    public Map<String, Object> getParams() {
      return params;
    }

    /** Convert this parameter set to a string representation for filenames */
    public String toFilenameSafeString() {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, Object> entry : params.entrySet()) {
        if (sb.length() > 0) {
          sb.append("_");
        }
        sb.append(entry.getKey()).append("-").append(entry.getValue());
      }
      return sb.toString();
    }
  }

  /**
   * Generate all combinations of scheduler parameters for grid search. Similar to the Python
   * implementation in runner.py.
   */
  public static List<SchedulerParams> generateSchedulerCombinations(
      Map<String, List<Object>> paramOptions, int workers) {
    List<SchedulerParams> result = new ArrayList<>();

    // Generate the cartesian product of all parameter options
    List<String> keys = new ArrayList<>(paramOptions.keySet());
    generateCombinationsRecursive(paramOptions, keys, 0, new HashMap<>(), result, workers);

    return result;
  }

  private static void generateCombinationsRecursive(
      Map<String, List<Object>> paramOptions,
      List<String> keys,
      int keyIndex,
      Map<String, Object> currentCombination,
      List<SchedulerParams> result,
      int workers) {

    // TODO REMOVE
    workers = 1;
    if (keyIndex >= keys.size()) {
      // Check if the combination meets the condition: min_granularity_ns <= latency_ns / workers
      Object minGranularity =
          currentCombination.getOrDefault("min_granularity_ns", Float.POSITIVE_INFINITY);
      Object latency = currentCombination.getOrDefault("latency_ns", 0);

      if (minGranularity instanceof Number && latency instanceof Number) {
        double minGranularityVal = ((Number) minGranularity).doubleValue();
        double latencyVal = ((Number) latency).doubleValue() / workers;

        if (minGranularityVal <= latencyVal) {
          result.add(new SchedulerParams(new HashMap<>(currentCombination)));
        }
      } else {
        // If we don't have both parameters, just add the combination
        result.add(new SchedulerParams(new HashMap<>(currentCombination)));
      }
      return;
    }

    String currentKey = keys.get(keyIndex);
    List<Object> options = paramOptions.get(currentKey);

    for (Object option : options) {
      currentCombination.put(currentKey, option);
      generateCombinationsRecursive(
          paramOptions, keys, keyIndex + 1, currentCombination, result, workers);
    }
  }

  /** Apply scheduler parameters by writing to the appropriate files */
  private static void applySchedulerParams(SchedulerParams params) {
    LOG.info("Applying scheduler parameters: {}", params.toFilenameSafeString());

    for (Map.Entry<String, Object> param : params.getParams().entrySet()) {
      String paramPath = "/sys/kernel/debug/sched/" + param.getKey();
      try {
        File file = new File(paramPath);
        if (file.exists()) {
          FileWriter writer = new FileWriter(file);
          writer.write(String.valueOf(param.getValue()));
          writer.close();
          LOG.info("Set {} to {}", param.getKey(), param.getValue());
        } else {
          LOG.warn("Warning: Parameter file not found: {}", paramPath);
        }
      } catch (IOException e) {
        LOG.error("Error setting parameter {}: {}", param.getKey(), e.getMessage());
      }
    }

    // Wait for the changes to apply
    LOG.info("Waiting 2 seconds for scheduler parameters to apply...");
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Configure the scheduler parameters to test
   *
   * @param params Map of parameter names to lists of values to test
   * @param workers Number of workers (used for filtering invalid combinations)
   */
  public static void configureSchedulerParams(Map<String, List<Object>> params, int workers) {
    schedulerParamOptions = params;
    workerCount = workers;

    // Print more detailed information about the scheduler parameters
    StringBuilder paramDetails = new StringBuilder();
    paramDetails.append("Scheduler parameters configuration:\n");
    for (Map.Entry<String, List<Object>> entry : params.entrySet()) {
      paramDetails.append("  - ").append(entry.getKey()).append(": ");
      if (entry.getValue().size() == 1) {
        paramDetails.append(entry.getValue().get(0));
      } else {
        paramDetails.append(entry.getValue());
      }
      paramDetails.append("\n");
    }
    LOG.info(paramDetails.toString());

    LOG.info(
        "Configured scheduler parameter search with {} parameters and {} workers",
        params.size(),
        workers);

    // Log the total required time for all combinations
    int requiredTime = calculateTotalRequiredTime();
    LOG.info("Total required time for all parameter combinations: {} seconds", requiredTime);
  }

  /**
   * Configure the measurement window length
   *
   * @param seconds Length of the measurement window in seconds
   */
  public static void configureMeasurementWindowSeconds(int seconds) {
    MEASUREMENT_WINDOW_SECONDS = seconds;
    LOG.info("Set measurement window length to {} seconds", seconds);
    LOG.info("Each parameter combination will be measured for exactly {} seconds", seconds);
  }

  /**
   * Start performance counters with various scheduler parameter configurations. This method starts
   * a background thread that handles parameter changes and measurements without blocking the main
   * benchmark thread.
   */
  public static void startPerfCounters(String baseFileName) {
    // Initialize if needed
    if (executor == null) {
      executor = Executors.newCachedThreadPool();
    }

    // Use the same base directory as other results
    String baseDirectory = "results";
    // Extract directory from baseFileName if present
    if (baseFileName.contains("/")) {
      int lastSlash = baseFileName.lastIndexOf('/');
      baseDirectory = baseFileName.substring(0, lastSlash);
    }

    // Create a perf subdirectory within the main results directory
    final String outputDirectory = baseDirectory + "/perf";
    FileUtil.makeDirIfNotExists(outputDirectory);

    // Print the absolute path of the perf results directory
    try {
      File outputDirFile = new File(outputDirectory);
      String absOutputPath = outputDirFile.getAbsolutePath();
      LOG.info("Performance measurement files will be stored in: {}", absOutputPath);
    } catch (Exception e) {
      LOG.warn("Could not determine absolute path for perf results directory");
    }

    // Create a daemon thread that will handle all scheduler parameter changes and perf measurements
    if (perfThread != null && perfThread.isAlive()) {
      LOG.info("Performance monitoring thread is already running, not starting a new one");
      return;
    }

    // Clear any previous measurement windows
    measurementWindows.clear();

    perfThread =
        new Thread(
            () -> {
              final String threadBaseFileName = baseFileName;
              LOG.info("Starting performance measurement thread");

              // Use configured parameters or default if not set
              Map<String, List<Object>> paramOptionsToUse = schedulerParamOptions;
              if (paramOptionsToUse.isEmpty()) {
                // Define default scheduler parameters to test
                paramOptionsToUse = new HashMap<>();
                paramOptionsToUse.put("min_granularity_ns", Arrays.asList(10000, 20000000));
                paramOptionsToUse.put("latency_ns", Arrays.asList(10000, 20000000));
                LOG.info("Using default scheduler parameter options");
              }

              // Generate all valid combinations
              List<SchedulerParams> paramCombinations =
                  generateSchedulerCombinations(paramOptionsToUse, workerCount);

              LOG.info("Testing {} scheduler parameter combinations", paramCombinations.size());

              // Run each combination
              for (SchedulerParams params : paramCombinations) {
                // Skip if shutdown was requested
                if (shutdownRequested) {
                  break;
                }

                // Apply the scheduler parameters
                applySchedulerParams(params);

                // Create unique filenames based on parameters
                String paramStr = params.toFilenameSafeString();
                String perfStatOutput =
                    outputDirectory + "/" + threadBaseFileName + "." + paramStr + ".perf.stat.txt";
                String perfRecordOutput =
                    outputDirectory
                        + "/"
                        + threadBaseFileName
                        + "."
                        + paramStr
                        + ".perf.record.txt";

                LOG.info("Starting perf measurements with configuration: {}", paramStr);
                LOG.info("Creating performance files:");
                LOG.info("  - Perf stat file: {}", perfStatOutput);
                LOG.info("  - Perf record file: {}", perfRecordOutput);

                // Print detailed parameter information for this measurement window
                StringBuilder paramDetails = new StringBuilder();
                paramDetails.append("Current parameter set details:\n");
                for (Map.Entry<String, Object> entry : params.getParams().entrySet()) {
                  paramDetails
                      .append("  - ")
                      .append(entry.getKey())
                      .append(": ")
                      .append(entry.getValue())
                      .append("\n");
                }
                LOG.info(paramDetails.toString());

                // Define measurement duration (in seconds)
                final int measurementWindow = MEASUREMENT_WINDOW_SECONDS;
                Collection<Process> currentMeasurementProcesses = new ArrayList<>();

                // Record the measurement start time
                final long measurementStartMs = System.currentTimeMillis();
                final long measurementEndMs = measurementStartMs + (measurementWindow * 1000L);

                // Store the measurement window for later histogram generation
                final String windowId = paramStr;
                measurementWindows.put(
                    windowId,
                    new MeasurementWindow(
                        windowId, measurementStartMs, measurementEndMs, params.getParams()));

                LOG.info(
                    "Recording transactions between {} and {} ms for parameter set: {}",
                    measurementStartMs,
                    measurementEndMs,
                    paramStr);

                // Run perf stat and perf sched record in parallel
                Process statProcess = null;
                Process schedProcess = null;

                try {
                  // Start perf stat
                  String[] statCommand = {
                    "sudo",
                    "perf",
                    "stat",
                    "-a",
                    "-o",
                    perfStatOutput,
                    "--",
                    "sleep",
                    String.valueOf(measurementWindow)
                  };
                  ProcessBuilder statPb = new ProcessBuilder(statCommand);
                  statProcess = statPb.start();
                  currentMeasurementProcesses.add(statProcess);
                  activeProcesses.add(statProcess);

                  // Start perf sched record
                  String[] schedCommand = {
                    "sudo",
                    "perf",
                    "sched",
                    "record",
                    "-a",
                    "-o",
                    perfRecordOutput,
                    "--",
                    "sleep",
                    String.valueOf(measurementWindow)
                  };
                  ProcessBuilder schedPb = new ProcessBuilder(schedCommand);
                  schedProcess = schedPb.start();
                  currentMeasurementProcesses.add(schedProcess);
                  activeProcesses.add(schedProcess);

                  // Wait for processes to complete
                  for (Process process : currentMeasurementProcesses) {
                    try {
                      process.waitFor();
                    } catch (InterruptedException e) {
                      // Ignore, will be handled in finally block
                    }
                  }

                  // Append timestamps to the perf stat output file
                  try {
                    FileWriter timestampWriter = new FileWriter(perfStatOutput, true);
                    timestampWriter.write("\n\n# Measurement window information:\n");
                    timestampWriter.write("# Start timestamp (ms): " + measurementStartMs + "\n");
                    timestampWriter.write("# End timestamp (ms): " + measurementEndMs + "\n");
                    timestampWriter.write("# Parameter set: " + paramStr + "\n");
                    timestampWriter.close();
                    LOG.info("Added timestamp information to perf output file");

                    // Create a separate CSV file with performance metrics and timestamps
                    String perfCsvOutput =
                        outputDirectory + "/" + threadBaseFileName + "." + paramStr + ".perf.csv";
                    LOG.info("Creating CSV file with performance metrics: {}", perfCsvOutput);
                    FileWriter csvWriter = new FileWriter(perfCsvOutput);

                    // Write CSV header
                    csvWriter.write("event_type,start_time_ms,end_time_ms,param_set_id");
                    // Add parameter columns
                    for (String paramName : params.getParams().keySet()) {
                      csvWriter.write("," + paramName);
                    }
                    csvWriter.write("\n");

                    // Write measurement window data
                    csvWriter.write(
                        String.format(
                            "measurement,%d,%d,%s",
                            measurementStartMs, measurementEndMs, windowId));

                    // Add parameter values
                    for (Map.Entry<String, Object> param : params.getParams().entrySet()) {
                      csvWriter.write("," + param.getValue());
                    }
                    csvWriter.write("\n");

                    csvWriter.close();
                    LOG.info(
                        "Created CSV file with performance metrics and timestamps: {}",
                        perfCsvOutput);
                  } catch (IOException e) {
                    LOG.error("Failed to append timestamps to perf output: {}", e.getMessage());
                  }

                  LOG.info("Completed perf measurements for {}", paramStr);
                } catch (Exception e) {
                  LOG.error("Error running perf measurements: {}", e.getMessage());
                } finally {
                  // Clean up any processes that might still be running
                  for (Process process : currentMeasurementProcesses) {
                    if (process.isAlive()) {
                      process.destroy();
                    }
                    activeProcesses.remove(process);
                  }
                }

                // Continue to next parameter set without blocking main thread
              }

              LOG.info("Performance measurement thread completed all parameter combinations");
            });

    // Set as daemon so it doesn't prevent JVM shutdown
    perfThread.setDaemon(true);
    perfThread.start();

    LOG.info("Started performance measurement thread in the background");
  }

  /**
   * Stop all active perf processes and shutdown the executor. This method is designed to be
   * non-blocking and will not affect the main benchmark results.
   */
  public static void stopPerfCounters() {
    // Signal the thread to stop
    shutdownRequested = true;

    // Create a separate cleanup thread so we don't block the caller
    Thread cleanupThread =
        new Thread(
            () -> {
              try {
                // Interrupt the perf thread if it's running
                if (perfThread != null && perfThread.isAlive()) {
                  try {
                    perfThread.interrupt();
                    // Wait briefly for the thread to terminate
                    perfThread.join(1000);
                  } catch (Exception e) {
                    // Ignore any errors
                  }
                }

                // Stop all active processes
                for (Iterator<Process> it = activeProcesses.iterator(); it.hasNext(); ) {
                  Process p = it.next();
                  try {
                    if (p.isAlive()) {
                      p.destroy();
                    }
                  } catch (Exception e) {
                    // Ignore errors
                  }
                  it.remove();
                }

                // Shutdown executor
                if (executor != null) {
                  executor.shutdown();
                  try {
                    if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                      executor.shutdownNow();
                    }
                  } catch (InterruptedException e) {
                    executor.shutdownNow();
                  }
                }

                LOG.info("All perf measurements cleanup completed");
              } catch (Exception e) {
                LOG.error("Error during perf cleanup: {}", e.getMessage());
              }
            });

    // Make it a daemon thread so it doesn't block JVM shutdown
    cleanupThread.setDaemon(true);
    cleanupThread.start();

    LOG.info("Initiated shutdown of perf measurements in background");
  }

  public Results(
      State state,
      long startTimestampMs,
      long elapsedNanoseconds,
      int measuredRequests,
      DistributionStatistics distributionStatistics,
      final List<LatencyRecord.Sample> latencySamples) {
    this.startTimestampMs = startTimestampMs;
    this.nanoseconds = elapsedNanoseconds;
    this.measuredRequests = measuredRequests;
    this.distributionStatistics = distributionStatistics;
    this.state = state;

    if (distributionStatistics == null) {
      this.latencySamples = null;
    } else {
      // defensive copy
      this.latencySamples = List.copyOf(latencySamples);
    }
  }

  public State getState() {
    return state;
  }

  public DistributionStatistics getDistributionStatistics() {
    return distributionStatistics;
  }

  public Histogram<TransactionType> getSuccess() {
    return success;
  }

  public Histogram<TransactionType> getUnknown() {
    return unknown;
  }

  public Histogram<TransactionType> getAbort() {
    return abort;
  }

  public Histogram<TransactionType> getRetry() {
    return retry;
  }

  public Histogram<TransactionType> getError() {
    return error;
  }

  public Histogram<TransactionType> getRetryDifferent() {
    return retryDifferent;
  }

  public Map<TransactionType, Histogram<String>> getAbortMessages() {
    return abortMessages;
  }

  public double requestsPerSecondThroughput() {
    return (double) measuredRequests / (double) nanoseconds * 1e9;
  }

  public double requestsPerSecondGoodput() {
    return (double) success.getSampleCount() / (double) nanoseconds * 1e9;
  }

  public List<Sample> getLatencySamples() {
    return latencySamples;
  }

  public long getStartTimestampMs() {
    return startTimestampMs;
  }

  public long getNanoseconds() {
    return nanoseconds;
  }

  public int getMeasuredRequests() {
    return measuredRequests;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Results(state=");
    sb.append(state);
    sb.append(", nanoSeconds=");
    sb.append(nanoseconds);
    sb.append(", measuredRequests=");
    sb.append(measuredRequests);
    sb.append(") = ");
    sb.append(requestsPerSecondThroughput());
    sb.append(" requests/sec (throughput)");
    sb.append(", ");
    sb.append(requestsPerSecondGoodput());
    sb.append(" requests/sec (goodput)");
    return sb.toString();
  }
}
