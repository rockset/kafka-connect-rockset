package rockset.utils;

import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

//
// RetriableTask encapsulates a runnable expression. If the runnable fails
// due to a retriable exception, the run method schedules it to be run after
// a short delay. If all the retries expire, run method sets the execution exception
// and fails

public class RetriableTask extends FutureTask<Void> {
  private static Logger log = LoggerFactory.getLogger(RetriableTask.class);

  private static final int MAX_RETRIES_COUNT = 5;
  private static final int INITIAL_DELAY = 250;
  private static final double JITTER_FACTOR = 0.2;

  private final Runnable runnable;
  private final BlockingExecutor executorService;

  private int numRetries = 0;
  private int delay = INITIAL_DELAY;

  public RetriableTask(BlockingExecutor executorService, Runnable runnable) {
    super(runnable, null);
    this.runnable = runnable;
    this.executorService = executorService;
  }

  private void retry(long jitterDelay) {
    executorService.schedule(this, jitterDelay, TimeUnit.MILLISECONDS);
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
      if (!(e instanceof RetriableException))  {
        setException(e);
        return;
      }

      ++numRetries;
      // if retries exhausted, set the exception and return
      if (numRetries > MAX_RETRIES_COUNT) {
        setException(e);
        return;
      }

      // schedule
      delay *= 2;
      long jitterDelay = jitter(delay);
      log.info(String.format("Encountered retriable error. Retry count: %s. Retrying in %s ms.",
          numRetries, jitterDelay), e);
      retry(jitterDelay);
    }
  }
}
