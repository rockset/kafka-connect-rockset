package rockset.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetriableTaskTest {
  private static final Logger log = LoggerFactory.getLogger(RetriableTaskTest.class);

  @Test
  public void testRetriableTask() throws Exception {
    log.info("Running testRetriableTask");

    BlockingExecutor blockingExecutor = Mockito.mock(BlockingExecutor.class);
    ExecutorService retryExecutorService = MoreExecutors.newDirectExecutorService();

    Runnable runnable =
        () -> {
          throw new RetriableException("retriable runnable");
        };

    RetriableTask retriableTask =
        new RetriableTask(blockingExecutor, retryExecutorService, runnable);

    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) {
                retriableTask.run();
                return null;
              }
            })
        .when(blockingExecutor)
        .submit(retriableTask);

    retriableTask.run();
    assertThrows(ExecutionException.class, () -> retriableTask.get());

    // should be retried 5 times
    Mockito.verify(blockingExecutor, Mockito.times(5)).submit(retriableTask);
  }

  @Test
  public void testRetriableTaskNoRetries() throws Exception {
    log.info("Running testRetriableTaskNoRetries");

    BlockingExecutor blockingExecutor = Mockito.mock(BlockingExecutor.class);
    ExecutorService retryExecutorService = MoreExecutors.newDirectExecutorService();
    Runnable runnable =
        () -> {
          log.info("Success!");
        };

    RetriableTask retriableTask =
        new RetriableTask(blockingExecutor, retryExecutorService, runnable);

    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) {
                Assertions.fail("task retried, was expected to succeed in first attempt!");
                return null;
              }
            })
        .when(blockingExecutor)
        .submit(retriableTask);

    retriableTask.run();

    // zero retries
    Mockito.verify(blockingExecutor, Mockito.times(0)).submit(retriableTask);
  }

  @Test
  public void testRetriableTaskNotRetriable() throws Exception {
    log.info("Running testRetriableTaskNotRetriable");

    BlockingExecutor blockingExecutor = Mockito.mock(BlockingExecutor.class);
    ExecutorService retryExecutorService = MoreExecutors.newDirectExecutorService();
    Runnable runnable =
        () -> {
          throw new ConnectException("non retriable runnable");
        };

    RetriableTask retriableTask =
        new RetriableTask(blockingExecutor, retryExecutorService, runnable);

    retriableTask.run();

    assertThrows(ExecutionException.class, () -> retriableTask.get());

    // zero retries
    Mockito.verify(blockingExecutor, Mockito.times(0)).submit(retriableTask);
  }

  @Test
  public void testRetryRejected() throws Exception {
    log.info("Running testRetryRejected");

    BlockingExecutor blockingExecutor = Mockito.mock(BlockingExecutor.class);
    ExecutorService retryExecutorService = Mockito.mock(ExecutorService.class);

    Runnable runnable =
        () -> {
          throw new RetriableException("retriable runnable");
        };

    Mockito.doThrow(RejectedExecutionException.class)
        .when(retryExecutorService)
        .submit(Mockito.any(Runnable.class));

    RetriableTask retriableTask =
        new RetriableTask(blockingExecutor, retryExecutorService, runnable);

    retriableTask.run();

    assertThrows(ExecutionException.class, () -> retriableTask.get());

    // zero retries
    Mockito.verify(blockingExecutor, Mockito.times(0)).submit(retriableTask);
  }
}
