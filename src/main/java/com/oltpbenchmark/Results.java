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

public final class Results {

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
    System.out.println("Applying scheduler parameters: " + params.toFilenameSafeString());

    for (Map.Entry<String, Object> param : params.getParams().entrySet()) {
      String paramPath = "/sys/kernel/debug/sched/" + param.getKey();
      try {
        File file = new File(paramPath);
        if (file.exists()) {
          FileWriter writer = new FileWriter(file);
          writer.write(String.valueOf(param.getValue()));
          writer.close();
          System.out.println("Set " + param.getKey() + " to " + param.getValue());
        } else {
          System.err.println("Warning: Parameter file not found: " + paramPath);
        }
      } catch (IOException e) {
        System.err.println("Error setting parameter " + param.getKey() + ": " + e.getMessage());
      }
    }

    // Wait for the changes to apply
    System.out.println("Waiting 2 seconds for scheduler parameters to apply...");
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
    System.out.println(
        "Configured scheduler parameter search with "
            + params.size()
            + " parameters and "
            + workers
            + " workers");
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

    final String outputDirectory = "perfResults";
    FileUtil.makeDirIfNotExists(outputDirectory);

    // Create a daemon thread that will handle all scheduler parameter changes and perf measurements
    if (perfThread != null && perfThread.isAlive()) {
      System.out.println(
          "Performance monitoring thread is already running, not starting a new one");
      return;
    }

    perfThread =
        new Thread(
            () -> {
              final String threadBaseFileName = baseFileName;
              System.out.println("Starting performance measurement thread");

              // Use configured parameters or default if not set
              Map<String, List<Object>> paramOptionsToUse = schedulerParamOptions;
              if (paramOptionsToUse.isEmpty()) {
                // Define default scheduler parameters to test
                paramOptionsToUse = new HashMap<>();
                paramOptionsToUse.put(
                    "min_granularity_ns", Arrays.asList(1000000, 2000000, 3000000));
                paramOptionsToUse.put("latency_ns", Arrays.asList(10000000, 20000000));
                System.out.println("Using default scheduler parameter options");
              }

              // Generate all valid combinations
              List<SchedulerParams> paramCombinations =
                  generateSchedulerCombinations(paramOptionsToUse, workerCount);

              System.out.println(
                  "Testing " + paramCombinations.size() + " scheduler parameter combinations");

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

                System.out.println("Starting perf measurements with configuration: " + paramStr);

                // Define measurement duration (in seconds)
                final int measurementWindow = 15;
                Collection<Process> currentMeasurementProcesses = new ArrayList<>();

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

                  System.out.println("Completed perf measurements for " + paramStr);
                } catch (Exception e) {
                  System.err.println("Error running perf measurements: " + e.getMessage());
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

              System.out.println(
                  "Performance measurement thread completed all parameter combinations");
            });

    // Set as daemon so it doesn't prevent JVM shutdown
    perfThread.setDaemon(true);
    perfThread.start();

    System.out.println("Started performance measurement thread in the background");
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

                System.out.println("All perf measurements cleanup completed");
              } catch (Exception e) {
                System.err.println("Error during perf cleanup: " + e.getMessage());
              }
            });

    // Make it a daemon thread so it doesn't block JVM shutdown
    cleanupThread.setDaemon(true);
    cleanupThread.start();

    System.out.println("Initiated shutdown of perf measurements in background");
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
