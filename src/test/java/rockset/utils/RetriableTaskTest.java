package rockset.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class RetriableTaskTest {
  private static final Logger log = LoggerFactory.getLogger(RetriableTaskTest.class);

  @Test
  public void testRetriableTask() {
    log.info("Running testRetriableTask");

    BlockingExecutor blockingExecutor = Mockito.mock(BlockingExecutor.class);
    Runnable runnable = () -> { throw new RetriableException("retriable runnable"); };

    RetriableTask retriableTask = new RetriableTask(blockingExecutor, runnable);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) {
        retriableTask.run();
        return null;
      }
    }).when(blockingExecutor).schedule(Mockito.any(), Mockito.anyLong(), Mockito.any());

    retriableTask.run();
    assertThrows(ExecutionException.class, () -> retriableTask.get());

    // should be retried 5 times
    Mockito.verify(blockingExecutor, Mockito.times(5)).schedule(Mockito.any(), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testRetriableTaskNoRetries() {
    log.info("Running testRetriableTaskNoRetries");

    BlockingExecutor blockingExecutor = Mockito.mock(BlockingExecutor.class);
    Runnable runnable = () -> { log.info("Success!"); };

    RetriableTask retriableTask = new RetriableTask(blockingExecutor, runnable);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) {
        Assertions.fail("task retried, was expected to succeed in first attempt!");
        return null;
      }
    }).when(blockingExecutor).schedule(Mockito.any(), Mockito.anyLong(), Mockito.any());

    retriableTask.run();

    // zero retries
    Mockito.verify(blockingExecutor, Mockito.times(0)).schedule(Mockito.any(), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testRetriableTaskNotRetriable() {
    log.info("Running testRetriableTaskNotRetriable");

    BlockingExecutor blockingExecutor = Mockito.mock(BlockingExecutor.class);
    Runnable runnable = () -> { throw new ConnectException("non retriable runnable"); };

    RetriableTask retriableTask = new RetriableTask(blockingExecutor, runnable);

    retriableTask.run();

    assertThrows(ExecutionException.class, () -> retriableTask.get());

    // zero retries
    Mockito.verify(blockingExecutor, Mockito.times(0)).schedule(Mockito.any(), Mockito.anyLong(), Mockito.any());
  }
}
