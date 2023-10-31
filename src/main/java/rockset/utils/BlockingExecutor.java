package rockset.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.apache.kafka.connect.errors.RetriableException;

/**
 * Use BlockingExecutor to block threads submitting tasks, when the executor is completely occupied.
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
  public Future<?> submit(Runnable task) {
    try {
      semaphore.acquire();
    } catch (InterruptedException unused) {
      CompletableFuture<Void> a = new CompletableFuture<>();
      a.completeExceptionally(new RetriableException("Thread interrupted"));
      return a;
    }
    return executorService.submit(
        () -> {
          try {
            task.run();
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
