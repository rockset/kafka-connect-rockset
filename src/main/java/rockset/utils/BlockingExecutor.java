package rockset.utils;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Use BlockingExecutor to block threads submitting tasks,
 * when the executor is completely occupied.
 */
public class BlockingExecutor {

  private final Semaphore semaphore;
  private final ScheduledExecutorService executorService;

  public BlockingExecutor(int numThreads, ScheduledExecutorService executorService) {
    this.semaphore = new Semaphore(numThreads);
    this.executorService = executorService;
  }

  // returns immediately if a thread is available to run the task,
  // else blocks until one of the active tasks completes
  public Future<?> submit(RetriableTask task) throws InterruptedException {
    semaphore.acquire();
    return executorService.submit(() -> {
      try {
        task.run();
      } finally {
        semaphore.release();
      }
    });
  }

  public void schedule(FutureTask<Void> task, long delay, TimeUnit timeUnit) {
    executorService.schedule(task, delay, timeUnit);
  }

  public void shutdown() {
    executorService.shutdown();
  }

  //
  // Force shutdown
  //
  public void shutdownNow() {
    executorService.shutdownNow();
  }
}
