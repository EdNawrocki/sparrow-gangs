/*
 * Copyright 2013 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.sparrow.examples;

import java.util.Set;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Simple frontend that runs jobs composed of sleep tasks.
 */
public class SimpleFrontend implements FrontendService.Iface {
  /** Amount of time to launch tasks for. */
  public static final String EXPERIMENT_S = "experiment_s";
  public static final int DEFAULT_EXPERIMENT_S = 10; // Changed experiment duration to ten seconds for quicker testing
  public static final int NUMBER_OF_MESSAGES  = 100; // Number of messages we will send out
  public static final double DEFAULT_GANG_RATE = .2; // Probability is currently 20%

  public static final String JOB_ARRIVAL_PERIOD_MILLIS = "job_arrival_period_millis";
  public static final int DEFAULT_JOB_ARRIVAL_PERIOD_MILLIS = 100;

  /** Number of tasks per job. */
  public static final String TASKS_PER_JOB = "tasks_per_job";
  public static final int DEFAULT_TASKS_PER_JOB = 1;

  /** Duration of one task, in milliseconds */
  public static final String TASK_DURATION_MILLIS = "task_duration_millis";
  public static final int DEFAULT_TASK_DURATION_MILLIS = 100;

  /** Host and port where scheduler is running. */
  public static final String SCHEDULER_HOST = "scheduler_host";
  public static final String DEFAULT_SCHEDULER_HOST = "localhost";
  public static final String SCHEDULER_PORT = "scheduler_port";

  private boolean shouldBePaused; // When we schedule a gang

  int messagesSeen = 0;
  int messagesSent = 0;
  Set<Integer> hashSet = new HashSet<>();

  /**
   * Default application name.
   */
  public static final String APPLICATION_ID = "sleepApp";

  private static final Logger LOG = Logger.getLogger(SimpleFrontend.class);

  private static final TUserGroupInfo USER = new TUserGroupInfo();

  private SparrowFrontendClient client;

  public int cur_id = 0; // Used to assign a unique ID to all users

  /** A runnable which Spawns a new thread to launch a scheduling request. */
  private class JobLaunchRunnable implements Runnable {
    private int tasksPerJob;
    private int taskDurationMillis;

    public JobLaunchRunnable(int tasksPerJob, int taskDurationMillis) {
      this.tasksPerJob = tasksPerJob;
      this.taskDurationMillis = taskDurationMillis;
    }

    @Override
    public void run() {
      // Generate tasks in the format expected by Sparrow. First, pack task parameters.
      ByteBuffer message = ByteBuffer.allocate(4);
      message.putInt(taskDurationMillis);

      // Set the number of tasks to a random number between 1 and 2
      Random random = new Random();
      int tasksPerJob = random.nextInt(2) + 1;

      if (NUMBER_OF_MESSAGES-cur_id == 1)
        tasksPerJob = 1;

      LOG.debug("Tasks per job: " + tasksPerJob);

      boolean is_gang = false;

      // 2*DEFAULT_GANG_RATE because we only gang schedule jobs of size 2
      if (tasksPerJob == 2 && Math.random() < (2*DEFAULT_GANG_RATE))
        is_gang = true;

      List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
      for (int taskId = 0; taskId < tasksPerJob; taskId++) {
        messagesSent++;
        TTaskSpec spec = new TTaskSpec();
        
        if (is_gang) {
          spec.setTaskId("G_" + Integer.toString(cur_id));
          LOG.debug("Gang with ID: " + cur_id);

          // This message must be received before we continue
          hashSet.add(cur_id);
        }

        else {
          spec.setTaskId(Integer.toString(cur_id));
          LOG.debug("No Gang with ID: " + cur_id);
        }

        cur_id++;

        spec.setMessage(message.array());
        tasks.add(spec);
      }
      long start = System.currentTimeMillis();
      try {
        client.submitJob(APPLICATION_ID, tasks, USER);
      } catch (TException e) {
        LOG.error("Scheduling request failed!", e);
      }
      long end = System.currentTimeMillis();
      LOG.debug("Scheduling request duration " + (end - start));

      shouldBePaused = is_gang;
    }
  }

  public void run(String[] args) {
    try {
      OptionParser parser = new OptionParser();
      parser.accepts("c", "configuration file").withRequiredArg().ofType(String.class);
      parser.accepts("help", "print help statement");
      OptionSet options = parser.parse(args);

      if (options.has("help")) {
        parser.printHelpOn(System.out);
        System.exit(-1);
      }

      // Logger configuration: log to the console
      BasicConfigurator.configure();
      LOG.setLevel(Level.DEBUG);

      Configuration conf = new PropertiesConfiguration();

      if (options.has("c")) {
        String configFile = (String) options.valueOf("c");
        conf = new PropertiesConfiguration(configFile);
      }

      int arrivalPeriodMillis = conf.getInt(JOB_ARRIVAL_PERIOD_MILLIS,
          DEFAULT_JOB_ARRIVAL_PERIOD_MILLIS);
      int experimentDurationS = conf.getInt(EXPERIMENT_S, DEFAULT_EXPERIMENT_S);
      LOG.debug("Using arrival period of " + arrivalPeriodMillis +
          " milliseconds and running experiment for " + experimentDurationS + " seconds.");
      int tasksPerJob = conf.getInt(TASKS_PER_JOB, DEFAULT_TASKS_PER_JOB);
      int taskDurationMillis = conf.getInt(TASK_DURATION_MILLIS, DEFAULT_TASK_DURATION_MILLIS);

      int schedulerPort = conf.getInt(SCHEDULER_PORT,
          SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
      String schedulerHost = conf.getString(SCHEDULER_HOST, DEFAULT_SCHEDULER_HOST);
      client = new SparrowFrontendClient();
      client.initialize(new InetSocketAddress(schedulerHost, schedulerPort), APPLICATION_ID, this);

      JobLaunchRunnable runnable = new JobLaunchRunnable(tasksPerJob, taskDurationMillis);
      ScheduledThreadPoolExecutor taskLauncher = new ScheduledThreadPoolExecutor(1);
      PausableScheduler pausableScheduler = new PausableScheduler(taskLauncher);
      pausableScheduler.scheduleAtFixedRate(runnable, 0, arrivalPeriodMillis, TimeUnit.MILLISECONDS);

      long trueStartTime = System.currentTimeMillis();
      LOG.debug("sleeping");

      while (messagesSent < NUMBER_OF_MESSAGES) {
        if (shouldBePaused && !pausableScheduler.isPaused())
          pausableScheduler.pause();

        if (!shouldBePaused && pausableScheduler.isPaused())
          pausableScheduler.resume();

        Thread.sleep(1);
      }

      taskLauncher.shutdown();

      while (messagesSeen < messagesSent)
        Thread.sleep(1);

      // Calculate elapsed time
      long currentTime = System.currentTimeMillis();
      long elapsedTime = currentTime - trueStartTime;

      // Print elapsed time in milliseconds
      System.out.println("Time elapsed: " + elapsedTime + " ms");
      return;
    }
    catch (Exception e) {
      LOG.error("Fatal exception", e);
    }
  }

  @Override
  public void frontendMessage(TFullTaskId taskId, int status, ByteBuffer message)
      throws TException {
    // We don't use messages here, so just log it.
    byte[] bytes = Serialization.getByteBufferContents(message);
    ByteBuffer readBuffer = ByteBuffer.wrap(bytes);
    int result = readBuffer.getInt();
    System.out.println("Got message: " + result);
    
    // Remove the message from the hashset if seen
    if (hashSet.contains(result))
        hashSet.remove(result);

    if (hashSet.isEmpty())
        shouldBePaused = false;

    messagesSeen++;
  }

  public static void main(String[] args) {
    new SimpleFrontend().run(args);
  }
}
