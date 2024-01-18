import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.exception.RawCASConflictException;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.gson.Gson;
import org.tikv.shade.com.google.gson.annotations.Expose;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.tikv.shade.io.prometheus.client.Counter;

public class Flow implements Runnable, Closeable {

  private static final Logger logger = LoggerFactory.getLogger(Bench.class);
  private static final long ttl = 300;
  private final RawKVClient client;
  private final String prefix;
  private final String reportPath;
  private final long duration;
  private final int threads;
  private final ExecutorService executorService;
  private final ReentrantLock mutex = new ReentrantLock();
  private Result result = null;

  public static final Counter REQUEST_FAILURE = Counter.build()
      .name("client_java_bench_requests_fail")
      .help("request failure counter")
      .labelNames("type")
      .register();

  private static final String typeGet = "get";
  private static final String typePutIfAbsent = "putIfAbsent";
  private static final String typePut = "put";


  private Flow(RawKVClient client, String prefix, String reportPath, long duration, int threads) {
    this.client = client;
    this.reportPath = reportPath;
    this.duration = duration;
    this.threads = threads;
    this.prefix = prefix;
    this.executorService = Executors.newFixedThreadPool(threads);
  }

  public void timeout() {
    try {
      Thread.sleep(duration * 1000);
    } catch (Exception ignored) {
    }
  }

  public static class Builder {

    private RawKVClient client;
    private String prefix;
    private String reportPath;
    private long duration;
    private int threads;

    public Builder client(RawKVClient client) {
      this.client = client;
      return this;
    }

    public Builder threads(int threads) {
      this.threads = threads;
      return this;
    }

    public Builder duration(long duration) {
      this.duration = duration;
      return this;
    }

    public Builder reportPath(String reportPath) {
      this.reportPath = reportPath;
      return this;
    }

    public Builder prefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Flow build() {
      return new Flow(client, prefix, reportPath, duration, threads);
    }
  }

  @Override
  public void run() {
    logger.info("start to run workload, duration: " + duration + "s, threads: " + threads);
    List<Future<?>> tasks = new ArrayList<>();
    AtomicBoolean stop = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(threads);
    for (int i = 0; i < threads; i++) {
      tasks.add(executorService.submit(() -> {
        task(stop);
        logger.info("thread " + Thread.currentThread().getId() + " finished");
        latch.countDown();
      }));
    }
    timeout();
    stop.set(true);
    try {
      latch.await();
    } catch (Exception ignored) {
    }
    logger.info("all threads finished");
  }

  void task(AtomicBoolean stop) {
    long id = 0;
    while (true) {
      if (stop.get()) {
        break;
      }
      try {
        transaction(id);
      } catch (Exception e) {
        mutex.lock();
        if (result != null) {
          String s = new Gson().toJson(result);
          try {
            File file = new File(this.reportPath);
            try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(file))) {
              fileWriter.write(s);
            } catch (IOException ignored) {
              logger.error("failed to write report file");
            }
          } catch (Exception ignored) {
            logger.error("failed to open report file");
          }
          System.exit(1);
        }
        mutex.unlock();
      }
      id++;
    }
  }

  void transaction(long id) {
    Random rand = new Random();
    int randomNum = rand.nextInt(6);
    ByteString key = ByteString.copyFromUtf8(
        prefix + randomNum + ":" + id + ":" + Thread.currentThread().getId());
    try {
      client.get(key);
    } catch (Exception e) {
      REQUEST_FAILURE.labels(typeGet).inc();
      logTxnErr(typeGet, key.toStringUtf8(), e);
      if (isFatalError(e)) {
        throw e;
      }
    }

    if (Math.random() < 0.5) {
      byte [] v = new byte[1024];
      Arrays.fill(v, (byte)79);
      ByteString value = ByteString.copyFrom(v);
      try {
        client.put(ByteString.copyFromUtf8("uxxxx").concat(key), value);
        client.put(ByteString.copyFromUtf8("vxxxx").concat(key), value);
        client.put(ByteString.copyFromUtf8("yxxxx").concat(key), value);
        client.put(ByteString.copyFromUtf8("zxxxx").concat(key), value);
      } catch (Exception e) {
        REQUEST_FAILURE.labels(typePut).inc();
        logTxnErr(typePut, key.toStringUtf8(), e);
        if (isFatalError(e)) {
          throw e;
        }
      }
    }

    // 1/6 putIfAbsent
    if (Math.random() < 0.16666666666666666) {
      try {
        client.putIfAbsent(key, key, ttl);
      } catch (RawCASConflictException ignored) {
      } catch (Exception e) {
        REQUEST_FAILURE.labels(typePutIfAbsent).inc();
        logTxnErr(typePutIfAbsent, key.toStringUtf8(), e);
        if (isFatalError(e)) {
          throw e;
        }
      }

      if (Math.random() < 0.5) {
        try {
          client.put(key, key, ttl);
        } catch (Exception e) {
          REQUEST_FAILURE.labels(typePut).inc();
          logTxnErr(typePut, key.toStringUtf8(), e);
          if (isFatalError(e)) {
            throw e;
          }
        }
      }
    }
  }

  void logTxnErr(String method, String key, Exception e) {
    logger.error(errMsg(method, key, e), e);
  }

  String errMsg(String method, String key, Exception e) {
    return String.format("encounter error while %s [key=%s] [err=%s]", method, key, e.getMessage());
  }

  boolean isFatalError(Exception e) {
    String err = e.getMessage();
    if (err.contains("key_not_in_region") || err.contains("key corrupted")) {
      mutex.lock();
      if (result == null) {
        result = new Result(1, err);
      }
      mutex.unlock();
      return true;
    }
    return false;
  }

  public static class Result {

    @Expose
    public int exitCode;
    @Expose
    public String message;

    public Result(int exitCode, String message) {
      this.exitCode = exitCode;
      this.message = message;
    }
  }

  @Override
  public void close() throws IOException {
    executorService.shutdown();
    client.close();
  }
}
