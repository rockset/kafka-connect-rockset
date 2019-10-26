package rockset.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * Use BlockingExecutor to block threads submitting tasks,
 * when the executor is completely occupied.
 */
public class BlockingExecutor {

  private final Semaphore semaphore;
  private final ExecutorService executorService;

  public BlockingExecutor(int numThreads, ExecutorService executorService) {
    this.semaphore = new Semaphore(numThreads);
    this.executorService = executorService;
  }

  // returns immediately if a thread is available to run the task,
  // else blocks until one of the active tasks completes
  public Future<?> submit(Runnable runnable) throws InterruptedException {
    semaphore.acquire();
    return executorService.submit(() -> {
      try {
        runnable.run();
      } finally {
        semaphore.release();
      }
    });
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
