package rockset.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingExecutorTest {

  private static final Logger log = LoggerFactory.getLogger(BlockingExecutorTest.class);

  @Test
  public void testBlockingExecutor() throws Exception {
    log.info("Running testBlockingExecutor");

    BlockingExecutor executor = new BlockingExecutor(2, Executors.newFixedThreadPool(2));

    CountDownLatch start1 = new CountDownLatch(1);
    CountDownLatch start2 = new CountDownLatch(1);

    // Both should go through
    Future f1 = executor.submit(() -> wait(start1));
    Future f2 = executor.submit(() -> wait(start2));

    // finish and wait for second task
    start2.countDown();
    f2.get(1, TimeUnit.SECONDS);

    // add another task
    CountDownLatch start3 = new CountDownLatch(1);
    Future f3 = executor.submit(() -> wait(start3));

    // finish and wait for third task
    start3.countDown();
    f3.get(1, TimeUnit.SECONDS);

    // finish and wait for first task
    start1.countDown();
    f1.get(1, TimeUnit.SECONDS);

    executor.shutdown();
  }

  @Test
  public void testBlockingExecutorBlocksWhenFull() throws Exception {
    log.info("Running testBlockingExecutorBlocksWhenFull");
    BlockingExecutor executor = new BlockingExecutor(1, Executors.newFixedThreadPool(1));

    CountDownLatch waitLatch = new CountDownLatch(1);
    executor.submit(() -> wait(waitLatch));

    // submitting a new job from a different thread should block the new thread
    CountDownLatch doneLatch = new CountDownLatch(1);
    Future secondTaskSubmission =
        Executors.newFixedThreadPool(1)
            .submit(
                () -> {
                  executor.submit(doneLatch::countDown);
                });

    try {
      secondTaskSubmission.get(1, TimeUnit.SECONDS);
      Assertions.fail("second task was submitted, was expected to block");
    } catch (TimeoutException e) {
      // this should happen
    }

    // let the first task finsh now
    waitLatch.countDown();

    // second task should now be submitted
    secondTaskSubmission.get(1, TimeUnit.SECONDS);

    // wait until second task is completed
    doneLatch.await();

    executor.shutdown();
  }

  private static void wait(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
