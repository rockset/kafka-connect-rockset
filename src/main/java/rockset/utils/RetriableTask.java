package rockset.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//
// RetriableTask encapsulates a runnable expression. If the runnable fails
// due to a retriable exception, the run method submits it to another thread pool
// to submit it to task thread pool to run after some time.
// If all the retries expire, run method sets the execution exception
// and fails.
// If the the retry queue is full, it will reject the task (retries are best effort)
//

public class RetriableTask extends FutureTask<Void> {
  private static Logger log = LoggerFactory.getLogger(RetriableTask.class);

  private static final int MAX_RETRIES_COUNT = 5;
  private static final int INITIAL_DELAY = 250;
  private static final double JITTER_FACTOR = 0.2;

  private final Runnable runnable;
  private final BlockingExecutor taskExecutorService;
  private final ExecutorService retryExecutorService;

  private int numRetries = 0;
  private int delay = INITIAL_DELAY;

  public RetriableTask(
      BlockingExecutor taskExecutorService,
      ExecutorService retryExecutorService,
      Runnable runnable) {

    super(runnable, null);
    this.taskExecutorService = taskExecutorService;
    this.retryExecutorService = retryExecutorService;
    this.runnable = runnable;
  }

  private void retry(Throwable retryException) {
    delay *= 2;
    long jitterDelay = jitter(delay);
    Long retryTime = System.currentTimeMillis() + jitterDelay;

    log.warn(
        String.format(
            "Encountered retriable error. Retry count: %s. Retrying in %s ms.",
            numRetries, jitterDelay),
        retryException);

    Runnable runnable =
        () -> {
          try {
            Long sleepTime = retryTime - System.currentTimeMillis();
            if (sleepTime > 0) {
              Thread.sleep(jitterDelay);
            }

            taskExecutorService.submit(this);
          } catch (InterruptedException e) {
            throw new ConnectException("Failed to put records", e);
          }
        };

    try {
      retryExecutorService.submit(runnable);
    } catch (RejectedExecutionException e) {
      setException(e);
      return;
    }
  }

  private static long jitter(int delay) {
    double rnd = ThreadLocalRandom.current().nextDouble(-1, 1);
    return (long) (delay * (1 + JITTER_FACTOR * rnd));
  }

  @Override
  public void run() {
    try {
      runnable.run();

      // mark completion of the task
      set(null);
    } catch (Exception e) {
      // if not a retriable exception, set the exception and return
      if (!(e instanceof RetriableException)) {
        setException(e);
        return;
      }

      ++numRetries;
      // if retries exhausted, set the exception and return
      if (numRetries > MAX_RETRIES_COUNT) {
        setException(e);
        return;
      }

      retry(e);
    }
  }
}
