/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.oltpbenchmark;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.collectors.monitoring.Monitor;
import com.oltpbenchmark.api.collectors.monitoring.MonitorGen;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.JSONUtil;
import com.oltpbenchmark.util.MonitorInfo;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.TimeUtil;

public class ThreadBench implements Thread.UncaughtExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadBench.class);
  // Determines how long (in ms) to wait until monitoring thread rejoins the
  // main thread.
  private static final int MONITOR_REJOIN_TIME = 60000;

  private final BenchmarkState testState;
  private final List<? extends Worker<? extends BenchmarkModule>> workers;
  private final ArrayList<Thread> workerThreads;
  private final List<WorkloadConfiguration> workConfs;
  private final ArrayList<LatencyRecord.Sample> samples = new ArrayList<>();
  private final MonitorInfo monitorInfo;

  // For continuous reporting mode
  private boolean continuousReporting = false;
  private int continuousWindow = 15;
  private boolean continuousPerf = false;
  private int continuousBuffer = 0;

  private Monitor monitor = null;

  private ThreadBench(
      List<? extends Worker<? extends BenchmarkModule>> workers,
      List<WorkloadConfiguration> workConfs,
      MonitorInfo monitorInfo,
      boolean continuousReporting,
      int continuousWindow,
      boolean continuousPerf,
      int continuousBuffer) {
    this.workers = workers;
    this.workConfs = workConfs;
    this.workerThreads = new ArrayList<>(workers.size());
    this.monitorInfo = monitorInfo;
    this.testState = new BenchmarkState(workers.size() + 1);
    this.continuousReporting = continuousReporting;
    this.continuousWindow = continuousWindow;
    this.continuousPerf = continuousPerf;
    this.continuousBuffer = continuousBuffer;
  }

  public static Results runRateLimitedBenchmark(
      List<Worker<? extends BenchmarkModule>> workers,
      List<WorkloadConfiguration> workConfs,
      MonitorInfo monitorInfo,
      CommandLine argsLine) {
    ThreadBench bench = new ThreadBench(workers, workConfs, monitorInfo, false, 15, false, 0);
    return bench.runRateLimitedMultiPhase(argsLine);
  }

  public static Results runRateLimitedBenchmark(
      List<Worker<? extends BenchmarkModule>> workers,
      List<WorkloadConfiguration> workConfs,
      MonitorInfo monitorInfo,
      CommandLine argsLine,
      boolean continuousReporting,
      int continuousWindow) {
    ThreadBench bench =
        new ThreadBench(
            workers, workConfs, monitorInfo, continuousReporting, continuousWindow, false, 0);
    return bench.runRateLimitedMultiPhase(argsLine);
  }

  public static Results runRateLimitedBenchmark(
      List<Worker<? extends BenchmarkModule>> workers,
      List<WorkloadConfiguration> workConfs,
      MonitorInfo monitorInfo,
      CommandLine argsLine,
      boolean continuousReporting,
      int continuousWindow,
      boolean continuousPerf) {
    ThreadBench bench =
        new ThreadBench(
            workers,
            workConfs,
            monitorInfo,
            continuousReporting,
            continuousWindow,
            continuousPerf,
            0);
    return bench.runRateLimitedMultiPhase(argsLine);
  }

  public static Results runRateLimitedBenchmark(
      List<Worker<? extends BenchmarkModule>> workers,
      List<WorkloadConfiguration> workConfs,
      MonitorInfo monitorInfo,
      CommandLine argsLine,
      boolean continuousReporting,
      int continuousWindow,
      boolean continuousPerf,
      int continuousBuffer) {
    ThreadBench bench =
        new ThreadBench(
            workers,
            workConfs,
            monitorInfo,
            continuousReporting,
            continuousWindow,
            continuousPerf,
            continuousBuffer);
    return bench.runRateLimitedMultiPhase(argsLine);
  }

  private void createWorkerThreads() {

    for (Worker<?> worker : workers) {
      worker.initializeState();
      Thread thread = new Thread(worker);
      thread.setUncaughtExceptionHandler(this);
      thread.start();
      this.workerThreads.add(thread);
    }
  }

  private void interruptWorkers() {
    for (Worker<?> worker : workers) {
      worker.cancelStatement();
    }
  }

  private int finalizeWorkers(ArrayList<Thread> workerThreads) throws InterruptedException {

    int requests = 0;

    new WatchDogThread().start();

    for (int i = 0; i < workerThreads.size(); ++i) {

      // FIXME not sure this is the best solution... ensure we don't hang
      // forever, however we might ignore problems
      workerThreads.get(i).join(60000); // wait for 60second for threads
      // to terminate... hands otherwise

      /*
       * // CARLO: Maybe we might want to do this to kill threads that are hanging... if
       * (workerThreads.get(i).isAlive()) { workerThreads.get(i).kill(); try {
       * workerThreads.get(i).join(); } catch (InterruptedException e) { } }
       */

      requests += workers.get(i).getRequests();

      LOG.debug("threadbench calling teardown");

      workers.get(i).tearDown();
    }

    return requests;
  }

  private Results runRateLimitedMultiPhase(CommandLine argsLine) {
    boolean errorsThrown = false;
    List<WorkloadState> workStates = new ArrayList<>();

    for (WorkloadConfiguration workState : this.workConfs) {
      workState.initializeState(testState);
      workStates.add(workState.getWorkloadState());
    }

    this.createWorkerThreads();

    // long measureStart = start;
    Phase phase = null;

    // used to determine the longest sleep interval
    double lowestRate = Double.MAX_VALUE;

    for (WorkloadState workState : workStates) {
      workState.switchToNextPhase();
      phase = workState.getCurrentPhase();
      LOG.info(phase.currentPhaseString());
      if (phase.getRate() < lowestRate) {
        lowestRate = phase.getRate();
      }
    }

    // Change testState to cold query if execution is serial, since we don't
    // have a warm-up phase for serial execution but execute a cold and a
    // measured query in sequence.
    if (phase != null && phase.isLatencyRun()) {
      synchronized (testState) {
        testState.startColdQuery();
      }
    }

    long startTs = System.currentTimeMillis();
    long start = System.nanoTime();
    long warmupStart = System.nanoTime();
    long warmup = warmupStart;
    long measureEnd = -1;

    long intervalNs = getInterval(lowestRate, phase.getArrival());

    long nextInterval = start + intervalNs;
    int nextToAdd = 1;
    int rateFactor;

    boolean resetQueues = true;

    long delta = phase.getTime() * 1000000000L;
    boolean lastEntry = false;

    // Initialize the Monitor
    if (this.monitorInfo.getMonitoringInterval() > 0) {
      this.monitor =
          MonitorGen.getMonitor(
              this.monitorInfo, this.testState, this.workers, this.workConfs.get(0));
      this.monitor.start();
    }

    // Allow workers to start work.
    testState.blockForStart();

    String name = StringUtils.join(StringUtils.split(argsLine.getOptionValue("b"), ','), '-');
    String baseFileName = name + "_" + TimeUtil.getCurrentTimeString();

    // Check for barebones mode
    boolean bareBonesRun = argsLine.hasOption("barebones-run");

    // Start perf counters in background thread (non-blocking) if not in barebones mode
    if (!bareBonesRun) {
      // This starts the daemon thread that will cycle through all scheduler parameters
      LOG.info("Starting performance measurements in the background");
      Results.startPerfCounters(baseFileName);
    } else {
      LOG.info("Barebones run: skipping performance measurements");
    }

    // Start continuous reporting thread if enabled
    if (this.continuousReporting) {
      LOG.info("Starting continuous reporting thread with window size of {}s", continuousWindow);
      // Set up a simple background thread to collect and report statistics periodically
      Thread reportingThread =
          new Thread(
              () -> {
                LOG.info("Continuous reporting thread started");

                int windowNum = 0;
                while (true) {
                  try {
                    // Sleep for the specified window
                    Thread.sleep(continuousWindow * 1000);

                    // Check if test is complete
                    if (testState.getState() == State.EXIT || testState.getState() == State.DONE) {
                      break;
                    }

                    // Skip if we're not in the measurement phase yet
                    if (testState.getState() != State.MEASURE) {
                      continue;
                    }

                    // Collect current statistics from all workers
                    windowNum++;

                    // Record the start time of this measurement window
                    long windowStartMs = System.currentTimeMillis();
                    String windowStartTime = TimeUtil.getCurrentTimeString();

                    LOG.info(
                        "Collecting continuous report for window #{} at {}",
                        windowNum,
                        windowStartTime);

                    int totalRequests = 0;
                    int totalSuccesses = 0;
                    int totalAborts = 0;
                    int totalRetries = 0;
                    int totalErrors = 0;
                    List<Integer> latencies = new ArrayList<>();

                    // Collect transaction statistics
                    for (Worker<?> worker : workers) {
                      totalSuccesses += worker.getTransactionSuccessHistogram().getSampleCount();
                      totalAborts += worker.getTransactionAbortHistogram().getSampleCount();
                      totalRetries += worker.getTransactionRetryHistogram().getSampleCount();
                      totalErrors += worker.getTransactionErrorHistogram().getSampleCount();
                      totalRequests += worker.getRequests();

                      // Get latency samples directly from worker's latencies
                      for (LatencyRecord.Sample sample : worker.getLatencyRecords()) {
                        // Already in microseconds
                        latencies.add(sample.getLatencyMicrosecond());
                      }
                    }

                    // Record end time of this measurement
                    long windowEndMs = System.currentTimeMillis();
                    String windowEndTime = TimeUtil.getCurrentTimeString();
                    long actualWindowDurationMs = windowEndMs - windowStartMs;

                    // Calculate throughput and goodput
                    double throughput = totalRequests / (double) continuousWindow;
                    double goodput = totalSuccesses / (double) continuousWindow;

                    // Calculate latency percentiles if we have samples
                    double avgLatency = 0;
                    int minLatency = 0;
                    int maxLatency = 0;
                    int p25Latency = 0;
                    int p50Latency = 0;
                    int p75Latency = 0;
                    int p90Latency = 0;
                    int p99Latency = 0;

                    if (!latencies.isEmpty()) {
                      // Sort latencies for percentile calculation
                      try {
                        Collections.sort(latencies);
                      } catch (IllegalArgumentException e) {
                        LOG.warn(
                            "Error during latency sorting - using unsorted latencies: {}",
                            e.getMessage());
                        // Continue with unsorted latencies
                      }

                      // Calculate statistics
                      int sum = 0;
                      for (int lat : latencies) {
                        sum += lat;
                      }
                      avgLatency = sum / (double) latencies.size();
                      minLatency = latencies.get(0);
                      maxLatency = latencies.get(latencies.size() - 1);

                      // Calculate percentiles
                      p25Latency = getPercentile(latencies, 25);
                      p50Latency = getPercentile(latencies, 50);
                      p75Latency = getPercentile(latencies, 75);
                      p90Latency = getPercentile(latencies, 90);
                      p99Latency = getPercentile(latencies, 99);
                    }

                    // Prepare and write report file before logging
                    String fileBase = baseFileName.replace(".summary", "");

                    // Determine output directory
                    String outputDirectory = "results";
                    if (argsLine.hasOption("d")) {
                      outputDirectory = argsLine.getOptionValue("d");
                    }

                    // Create the directory if it doesn't exist
                    FileUtil.makeDirIfNotExists(outputDirectory);

                    String reportPath =
                        FileUtil.joinPath(
                            outputDirectory, fileBase + ".window" + windowNum + ".json");

                    Map<String, Object> report = new HashMap<>();
                    report.put("window", windowNum);
                    report.put("time_seconds", continuousWindow);
                    report.put("throughput", throughput);
                    report.put("goodput", goodput);
                    report.put("success_total", totalSuccesses);
                    report.put("requests_total", totalRequests);
                    report.put("abort_total", totalAborts);
                    report.put("retry_total", totalRetries);
                    report.put("error_total", totalErrors);
                    report.put(
                        "success_rate",
                        totalRequests > 0 ? (totalSuccesses * 100.0 / totalRequests) : 0.0);
                    report.put("latency_avg", avgLatency);
                    report.put("latency_min", minLatency);
                    report.put("latency_max", maxLatency);
                    report.put("latency_p25", p25Latency);
                    report.put("latency_p50", p50Latency);
                    report.put("latency_p75", p75Latency);
                    report.put("latency_p90", p90Latency);
                    report.put("latency_p99", p99Latency);
                    report.put("timestamp_start", windowStartTime);
                    report.put("timestamp_end", windowEndTime);
                    report.put("timestamp_start_ms", windowStartMs);
                    report.put("timestamp_end_ms", windowEndMs);
                    report.put("actual_duration_ms", actualWindowDurationMs);

                    // Add perf measurements if enabled
                    if (continuousPerf) {
                      try {
                        // Create a perf measurement for this window
                        String perfFileName = fileBase + ".window" + windowNum + ".perf";
                        String perfFilePath = FileUtil.joinPath(outputDirectory, perfFileName);

                        LOG.info("  Collecting performance metrics for window #{}", windowNum);

                        // Use ProcessBuilder instead of deprecated Runtime.exec
                        ProcessBuilder processBuilder =
                            new ProcessBuilder(
                                "perf",
                                "stat",
                                "-e",
                                "cycles,instructions,cache-references,cache-misses",
                                "-o",
                                perfFilePath,
                                "sleep",
                                "1");
                        Process process = processBuilder.start();
                        process.waitFor();

                        // Add perf file location to the report
                        report.put("perf_file", perfFileName);
                        LOG.info("  Performance metrics saved to: {}", perfFilePath);

                        // Write timestamp information to the perf file for later correlation
                        try {
                          String timestampInfo =
                              "# Measurement window: "
                                  + windowNum
                                  + "\n"
                                  + "# Start time: "
                                  + windowStartTime
                                  + " ("
                                  + windowStartMs
                                  + " ms)\n"
                                  + "# End time: "
                                  + windowEndTime
                                  + " ("
                                  + windowEndMs
                                  + " ms)\n"
                                  + "# Duration: "
                                  + actualWindowDurationMs
                                  + " ms\n";

                          FileUtil.appendStringToFile(new File(perfFilePath), timestampInfo);
                        } catch (Exception e) {
                          LOG.error("Error appending timestamp info to perf file", e);
                        }
                      } catch (Exception e) {
                        LOG.error("Error collecting performance metrics", e);
                      }
                    }

                    // Write report file immediately before printing to console
                    try {
                      FileUtil.writeStringToFile(
                          new File(reportPath), JSONUtil.toJSONString(report));
                    } catch (Exception e) {
                      LOG.error("Error writing window report", e);
                    }

                    // Now log everything to console
                    LOG.info("Window #{} Summary:", windowNum);
                    LOG.info("  Duration: {}s", continuousWindow);
                    LOG.info("  Throughput: {} txn/s", String.format("%.2f", throughput));
                    LOG.info("  Goodput: {} txn/s", String.format("%.2f", goodput));
                    LOG.info(
                        "  Success rate: {}%",
                        String.format(
                            "%.2f",
                            totalRequests > 0 ? (totalSuccesses * 100.0 / totalRequests) : 0.0));
                    LOG.info("  Transactions:");
                    LOG.info("    Successful: {}", totalSuccesses);
                    LOG.info("    Aborted:    {}", totalAborts);
                    LOG.info("    Retried:    {}", totalRetries);
                    LOG.info("    Errors:     {}", totalErrors);
                    LOG.info("    Total:      {}", totalRequests);
                    LOG.info("  Latency:");
                    LOG.info("    Average: {} us", String.format("%.2f", avgLatency));
                    LOG.info("    Minimum: {} us", minLatency);
                    LOG.info("    Maximum: {} us", maxLatency);
                    LOG.info("    p25: {} us", p25Latency);
                    LOG.info("    p50: {} us", p50Latency);
                    LOG.info("    p75: {} us", p75Latency);
                    LOG.info("    p90: {} us", p90Latency);
                    LOG.info("    p99: {} us", p99Latency);
                    LOG.info("  Window report written to: {}", reportPath);

                    // If a buffer time is specified, sleep for that period
                    if (continuousBuffer > 0) {
                      LOG.info("Waiting for buffer time of {}s between windows", continuousBuffer);
                      Thread.sleep(continuousBuffer * 1000);
                    }
                  } catch (InterruptedException e) {
                    break;
                  } catch (Exception e) {
                    LOG.error("Error in continuous reporting thread", e);
                  }
                }
                LOG.info("Continuous reporting thread finished");
              });

      reportingThread.setDaemon(true);
      reportingThread.start();
      LOG.info("Continuous reporting is now active");
    }

    // Main Loop
    while (true) {
      // posting new work... and resetting the queue in case we have new
      // portion of the workload...

      for (WorkloadState workState : workStates) {
        if (workState.getCurrentPhase() != null) {
          rateFactor = (int) (workState.getCurrentPhase().getRate() / lowestRate);
        } else {
          rateFactor = 1;
        }
        workState.addToQueue(nextToAdd * rateFactor, resetQueues);
      }
      resetQueues = false;

      // Wait until the interval expires, which may be "don't wait"
      long now = System.nanoTime();
      if (phase != null) {
        warmup = warmupStart + phase.getWarmupTime() * 1000000000L;
      }
      long diff = nextInterval - now;
      while (diff > 0) { // this can wake early: sleep multiple times to avoid that
        long ms = diff / 1000000;
        diff = diff % 1000000;
        try {
          Thread.sleep(ms, (int) diff);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        now = System.nanoTime();
        diff = nextInterval - now;
      }

      boolean phaseComplete = false;
      if (phase != null) {
        if (phase.isLatencyRun())
        // Latency runs (serial run through each query) have their own
        // state to mark completion
        {
          phaseComplete = testState.getState() == State.LATENCY_COMPLETE;
        } else {
          phaseComplete = testState.getState() == State.MEASURE && (start + delta <= now);
        }
      }

      // Go to next phase if this one is complete or enter if error was thrown
      boolean errorThrown = testState.getState() == State.ERROR;
      errorsThrown = errorsThrown || errorThrown;
      if ((phaseComplete || errorThrown) && !lastEntry) {
        // enters here after each phase of the test
        // reset the queues so that the new phase is not affected by the
        // queue of the previous one
        resetQueues = true;

        // Fetch a new Phase
        synchronized (testState) {
          if (phase.isLatencyRun()) {
            testState.ackLatencyComplete();
          }
          for (WorkloadState workState : workStates) {
            synchronized (workState) {
              workState.switchToNextPhase();
              lowestRate = Integer.MAX_VALUE;
              phase = workState.getCurrentPhase();
              interruptWorkers();
              if (phase == null && !lastEntry) {
                // Last phase
                lastEntry = true;
                testState.startCoolDown();
                measureEnd = now;
                LOG.info(
                    "{} :: Waiting for all terminals to finish ..", StringUtil.bold("TERMINATE"));
              } else if (phase != null) {
                // Reset serial execution parameters.
                if (phase.isLatencyRun()) {
                  phase.resetSerial();
                  testState.startColdQuery();
                }
                LOG.info(phase.currentPhaseString());
                if (phase.getRate() < lowestRate) {
                  lowestRate = phase.getRate();
                }
              }
            }
          }
          if (phase != null) {
            // update frequency in which we check according to
            // wakeup
            // speed
            // intervalNs = (long) (1000000000. / (double)
            // lowestRate + 0.5);
            delta += phase.getTime() * 1000000000L;
          }
        }
      }

      // Compute the next interval
      // and how many messages to deliver
      if (phase != null) {
        intervalNs = 0;
        nextToAdd = 0;
        do {
          intervalNs += getInterval(lowestRate, phase.getArrival());
          nextToAdd++;
        } while ((-diff) > intervalNs && !lastEntry);
        nextInterval += intervalNs;
      }

      // Update the test state appropriately
      State state = testState.getState();
      if (state == State.WARMUP && now >= warmup) {
        synchronized (testState) {
          if (phase != null && phase.isLatencyRun()) {
            testState.startColdQuery();
          } else {
            testState.startMeasure();
          }
          interruptWorkers();
        }
        start = now;
        LOG.info("{} :: Warmup complete, starting measurements.", StringUtil.bold("MEASURE"));

        // measureEnd = measureStart + measureSeconds * 1000000000L;

        // For serial executions, we want to do every query exactly
        // once, so we need to restart in case some of the queries
        // began during the warmup phase.
        // If we're not doing serial executions, this function has no
        // effect and is thus safe to call regardless.
        phase.resetSerial();
      } else if (state == State.EXIT) {
        // All threads have noticed the done, meaning all measured
        // requests have definitely finished.
        // Time to quit.
        break;
      }
    }

    // Stop the monitoring thread separately from cleanup all the workers so we can ignore errors
    // from these threads (including possible SQLExceptions), but not the others.
    try {
      if (this.monitor != null) {
        this.monitor.interrupt();
        this.monitor.join(MONITOR_REJOIN_TIME);
        this.monitor.tearDown();
      }

      // Stop any running perf counters if not in barebones mode
      if (!argsLine.hasOption("barebones-run")) {
        Results.stopPerfCounters();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    try {
      int requests = finalizeWorkers(this.workerThreads);

      // Combine all the latencies together in the most disgusting way
      // possible: sorting!
      for (Worker<?> w : workers) {
        for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
          samples.add(sample);
        }
      }

      // Use a try-catch block to handle potential sorting issues
      try {
        Collections.sort(samples);
      } catch (IllegalArgumentException e) {
        LOG.warn("Error during sample sorting - using unsorted samples: {}", e.getMessage());
        // Continue with unsorted samples
      }

      // Compute stats on all the latencies
      int[] latencies = new int[samples.size()];
      for (int i = 0; i < samples.size(); ++i) {
        latencies[i] = samples.get(i).getLatencyMicrosecond();
      }
      DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

      Results results =
          new Results(
              // If any errors were thrown during the execution, proprogate that fact to the
              // final Results state so we can exit non-zero *after* we output the results.
              errorsThrown ? State.ERROR : testState.getState(),
              startTs,
              measureEnd - start,
              requests,
              stats,
              samples);
      results.baseFileName = baseFileName;
      // Compute transaction histogram
      Set<TransactionType> txnTypes = new HashSet<>();
      for (WorkloadConfiguration workConf : workConfs) {
        txnTypes.addAll(workConf.getTransTypes());
      }
      txnTypes.remove(TransactionType.INVALID);

      results.getUnknown().putAll(txnTypes, 0);
      results.getSuccess().putAll(txnTypes, 0);
      results.getRetry().putAll(txnTypes, 0);
      results.getAbort().putAll(txnTypes, 0);
      results.getError().putAll(txnTypes, 0);
      results.getRetryDifferent().putAll(txnTypes, 0);

      for (Worker<?> w : workers) {
        results.getUnknown().putHistogram(w.getTransactionUnknownHistogram());
        results.getSuccess().putHistogram(w.getTransactionSuccessHistogram());
        results.getRetry().putHistogram(w.getTransactionRetryHistogram());
        results.getAbort().putHistogram(w.getTransactionAbortHistogram());
        results.getError().putHistogram(w.getTransactionErrorHistogram());
        results.getRetryDifferent().putHistogram(w.getTransactionRetryDifferentHistogram());
      }

      return (results);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private long getInterval(double lowestRate, Phase.Arrival arrival) {
    // TODO Auto-generated method stub
    if (arrival == Phase.Arrival.POISSON) {
      return (long) ((-Math.log(1 - Math.random()) / lowestRate) * 1000000000.);
    } else {
      return (long) (1000000000. / lowestRate + 0.5);
    }
  }

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    // Here we handle the case in which one of our worker threads died
    LOG.error(e.getMessage(), e);
    // We do not continue with the experiment. Instead, bypass rest of
    // phases that were left in the test and signal error state.
    // The rest of the workflow to finish the experiment remains the same,
    // and partial metrics will be reported (i.e., until failure happened).
    synchronized (testState) {
      for (WorkloadConfiguration workConf : this.workConfs) {
        synchronized (workConf.getWorkloadState()) {
          WorkloadState workState = workConf.getWorkloadState();
          Phase phase = workState.getCurrentPhase();
          while (phase != null) {
            workState.switchToNextPhase();
            phase = workState.getCurrentPhase();
          }
        }
      }
      testState.signalError();
    }
  }

  public static final class TimeBucketIterable implements Iterable<DistributionStatistics> {
    private final Iterable<Sample> samples;
    private final int windowSizeSeconds;
    private final TransactionType transactionType;

    /**
     * @param samples
     * @param windowSizeSeconds
     * @param transactionType Allows to filter transactions by type
     */
    public TimeBucketIterable(
        Iterable<Sample> samples, int windowSizeSeconds, TransactionType transactionType) {
      this.samples = samples;
      this.windowSizeSeconds = windowSizeSeconds;
      this.transactionType = transactionType;
    }

    @Override
    public Iterator<DistributionStatistics> iterator() {
      return new TimeBucketIterator(samples.iterator(), windowSizeSeconds, transactionType);
    }
  }

  private static final class TimeBucketIterator implements Iterator<DistributionStatistics> {
    private final Iterator<Sample> samples;
    private final int windowSizeSeconds;
    private final TransactionType txType;

    private Sample sample;
    private long nextStartNanosecond;

    private DistributionStatistics next;

    /**
     * @param samples
     * @param windowSizeSeconds
     * @param txType Allows to filter transactions by type
     */
    public TimeBucketIterator(
        Iterator<LatencyRecord.Sample> samples, int windowSizeSeconds, TransactionType txType) {
      this.samples = samples;
      this.windowSizeSeconds = windowSizeSeconds;
      this.txType = txType;

      if (samples.hasNext()) {
        sample = samples.next();
        // TODO: To be totally correct, we would want this to be the
        // timestamp of the start
        // of the measurement interval. In most cases this won't matter.
        nextStartNanosecond = sample.getStartNanosecond();
        calculateNext();
      }
    }

    private void calculateNext() {

      // Collect all samples in the time window
      ArrayList<Integer> latencies = new ArrayList<>();
      long endNanoseconds = nextStartNanosecond + (windowSizeSeconds * 1000000000L);
      while (sample != null && sample.getStartNanosecond() < endNanoseconds) {

        // Check if a TX Type filter is set, in the default case,
        // INVALID TXType means all should be reported, if a filter is
        // set, only this specific transaction
        if (txType.equals(TransactionType.INVALID)
            || txType.getId() == sample.getTransactionType()) {
          latencies.add(sample.getLatencyMicrosecond());
        }

        if (samples.hasNext()) {
          sample = samples.next();
        } else {
          sample = null;
        }
      }

      // Set up the next time window

      nextStartNanosecond = endNanoseconds;

      int[] l = new int[latencies.size()];
      for (int i = 0; i < l.length; ++i) {
        l[i] = latencies.get(i);
      }

      next = DistributionStatistics.computeStatistics(l);
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public DistributionStatistics next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      DistributionStatistics out = next;
      next = null;
      if (sample != null) {
        calculateNext();
      }
      return out;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("unsupported");
    }
  }

  private class WatchDogThread extends Thread {
    {
      this.setDaemon(true);
    }

    @Override
    public void run() {
      Map<String, Object> m = new ListOrderedMap<>();
      LOG.info("Starting WatchDogThread");
      while (true) {
        try {
          Thread.sleep(20000);
        } catch (InterruptedException ex) {
          return;
        }

        m.clear();
        for (Thread t : workerThreads) {
          m.put(t.getName(), t.isAlive());
        }
        LOG.info("Worker Thread Status:\n{}", StringUtil.formatMaps(m));
      }
    }
  }

  /**
   * Helper method to calculate percentiles from a sorted list
   *
   * @param sorted Sorted list of values
   * @param percentile Percentile to calculate (0-100)
   * @return The value at the given percentile
   */
  private static int getPercentile(List<Integer> sorted, int percentile) {
    if (sorted.isEmpty()) {
      return 0;
    }

    int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
    if (index < 0) {
      index = 0;
    }
    return sorted.get(index);
  }
}
