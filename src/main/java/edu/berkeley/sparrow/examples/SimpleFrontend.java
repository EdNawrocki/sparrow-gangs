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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.print.attribute.standard.NumberUpSupported;

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
  public static final String GANG_SCHED_RATE = "gang_schedule_rate";
  public static final double DEFAULT_GANG_RATE = 0.0; 

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

  /** Running multiple frontend instances.  */
  public static final String FRONT_END_INSTANCE = "front_end_instance";
  public static final String DEFAULT_FRONT_END_INSTANCE = "f1";

  /** Port for frontend to listen on.  */
  private final static String LISTEN_PORT = "listen_port";
  private final static int DEFAULT_LISTEN_PORT = 50201;

  private boolean shouldBePaused; // Determine if frontend should stall 

  /**
   * Default application name.
   */
  public static final String APPLICATION_ID = "sleepApp";

  private static final Logger LOG = Logger.getLogger(SimpleFrontend.class);

  private static final TUserGroupInfo USER = new TUserGroupInfo();

  private SparrowFrontendClient client;

  public int curId = 0; // Used to assign a unique ID to all users
  public int messagesSeen = 0;
  public int messagesSent = 0;
  public Set<String> hashSet = Collections.synchronizedSet(new HashSet<String>());

  /** A runnable which Spawns a new thread to launch a scheduling request. */
  private class JobLaunchRunnable implements Runnable {
    private int tasksPerJob;
    private int taskDurationMillis;
    private String frontendInstance;
    private double gangSchedRate;

    public JobLaunchRunnable(int tasksPerJob, int taskDurationMillis, String frontendInstance, double gangSchedRate) {
      this.tasksPerJob = tasksPerJob;
      this.taskDurationMillis = taskDurationMillis;
      this.frontendInstance = frontendInstance;
      this.gangSchedRate = gangSchedRate;
    }

    @Override
    public void run() {
      // Generate tasks in the format expected by Sparrow. First, pack task parameters.
      ByteBuffer message = ByteBuffer.allocate(4);
      message.putInt(taskDurationMillis);

      // Set the number of tasks from 1 to configured number randomly
      int curTasksPerJob = 0;

      if (NUMBER_OF_MESSAGES - curId <= tasksPerJob){
        curTasksPerJob = NUMBER_OF_MESSAGES - curId;
      }
      else {
        Random random = new Random();
        curTasksPerJob = random.nextInt(tasksPerJob) + 1;
      }

      LOG.debug("[NUM TASKS] in job: " + curTasksPerJob);

      boolean isGang = false;

      // Determine if we should gang schedule
      if (curTasksPerJob > 1 && Math.random() < gangSchedRate){
        isGang = true;
      }

      shouldBePaused = isGang;

      List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
      
      // Logging string 
      String taskIds = (isGang ? "[GANG]" : "[INDEPENDENT]") + " task IDs: ";

      // Set all tasks in job
      for (int i = 0; i < curTasksPerJob; i++) {
        messagesSent++;
        TTaskSpec spec = new TTaskSpec();
        
        if (isGang) {
          spec.setTaskId("G_" + frontendInstance + "_" + curId);
          // This message must be received before we continue
          hashSet.add(frontendInstance + "_" + curId);
        }
        else {
          spec.setTaskId(frontendInstance + "_" + curId);
        }
        taskIds += frontendInstance + "_" + curId + ", ";
        curId++;
        spec.setMessage(message.array());
        tasks.add(spec);
      }
      LOG.debug(taskIds.substring(0, taskIds.length()-2));

      long start = System.currentTimeMillis();
      try {
        client.submitJob(APPLICATION_ID, tasks, USER);
      } catch (TException e) {
        LOG.error("Scheduling request failed!", e);
      }
      long end = System.currentTimeMillis();
      LOG.debug("[SCHEDULING] request duration: " + (end - start) + " ms");
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
      String frontendInstance = conf.getString(FRONT_END_INSTANCE, DEFAULT_FRONT_END_INSTANCE);
      double gangSchedRate = conf.getDouble(GANG_SCHED_RATE, DEFAULT_GANG_RATE);

      int schedulerPort = conf.getInt(SCHEDULER_PORT,
          SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
      String schedulerHost = conf.getString(SCHEDULER_HOST, DEFAULT_SCHEDULER_HOST);
      int listenPort = conf.getInt(LISTEN_PORT, DEFAULT_LISTEN_PORT);
      client = new SparrowFrontendClient();
      client.initialize(new InetSocketAddress(schedulerHost, schedulerPort), APPLICATION_ID, this, listenPort);

      JobLaunchRunnable runnable = new JobLaunchRunnable(tasksPerJob, taskDurationMillis, frontendInstance, gangSchedRate);
      ScheduledThreadPoolExecutor taskLauncher = new ScheduledThreadPoolExecutor(1);
      PausableScheduler pausableScheduler = new PausableScheduler(taskLauncher);
      pausableScheduler.scheduleAtFixedRate(runnable, 0, arrivalPeriodMillis, TimeUnit.MILLISECONDS);

      long trueStartTime = System.currentTimeMillis();

      while (messagesSent < NUMBER_OF_MESSAGES) {
        if (shouldBePaused && !pausableScheduler.isPaused())
          pausableScheduler.pause();

        if (!shouldBePaused && pausableScheduler.isPaused())
          pausableScheduler.resume();

          Thread.sleep(1);
      }

      LOG.debug("All tasks in " + frontendInstance + " have been sent!");

      taskLauncher.shutdown();
      try {
          if (!taskLauncher.awaitTermination(3, TimeUnit.SECONDS)) {
              taskLauncher.shutdownNow();
          }
      } catch (InterruptedException e) {
          taskLauncher.shutdownNow(); // Force shutdown
      }

      while (messagesSeen < messagesSent)
        Thread.sleep(1);

      LOG.debug("All tasks in " + frontendInstance + " have been completed!");

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
    byte[] bytes = new byte[message.remaining()];
    message.get(bytes);
    String receivedTaskId = new String(bytes);
    LOG.debug("[RECEIVED] taskID: " + receivedTaskId + " with [HASHSET] size: " + hashSet.size());
    
    // Remove the message from the hashset if seen
    if (hashSet.contains(receivedTaskId))
        hashSet.remove(receivedTaskId);

    
    if (hashSet.isEmpty()){
        shouldBePaused = false;
    }
    else { // For debugging 
      String res = "";
      for (String s : hashSet){
        res += s + ", ";
      }
      LOG.debug("[HASHSET] contains: " + res.substring(0, res.length()-2));
    }

    messagesSeen++;
  }

  public static void main(String[] args) {
    new SimpleFrontend().run(args);
    System.exit(0);
  }
}
