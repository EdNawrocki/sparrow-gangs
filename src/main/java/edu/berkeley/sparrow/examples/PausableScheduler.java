package edu.berkeley.sparrow.examples;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class PausableScheduler {
    private final ScheduledThreadPoolExecutor executor;
    private final AtomicBoolean paused = new AtomicBoolean(false);
    
    public PausableScheduler(ScheduledThreadPoolExecutor executor) {
        this.executor = executor;
    }
    
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        Runnable pausableTask = () -> {
            if (!paused.get()) {
                command.run();
            }
        };
        return executor.scheduleAtFixedRate(pausableTask, initialDelay, period, unit);
    }
    
    public void pause() {
        paused.set(true);
    }
    
    public void resume() {
        paused.set(false);
    }

    public boolean isPaused() {
        return paused.get();
    }
}
