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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class Flow implements Runnable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Bench.class);
    private static final long ttl = 300;
    private final RawKVClient client;
    private final String prefix;
    private final long duration;
    private final int threads;
    private final ExecutorService executorService;
    private final ReentrantLock mutex = new ReentrantLock();
    private Result result = null;
    private static final byte[] infiniteEnd = new byte[]{
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
    };


    private Flow(RawKVClient client, String prefix, long duration, int threads) {
        this.client = client;
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

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Flow build() {
            return new Flow(client, prefix, duration, threads);
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
                    System.out.println(new Gson().toJson(result));
                }
                System.exit(1);
                mutex.unlock();
            }
            id++;
        }
    }

    void transaction(long id) {
        ByteString key = ByteString.copyFromUtf8(prefix + id + "_" + Thread.currentThread().getId());
        try {
            client.putIfAbsent(key, key, ttl);
            client.put(ByteString.copyFrom(new byte[]{0x00, 0x21}), key);
            client.put(ByteString.copyFrom(infiniteEnd), key);
        } catch (RawCASConflictException ignored) {
        } catch (Exception e) {
            logger.error(errMsg("putIfAbsent", key.toStringUtf8(), e));
            if (!handleError(e)) {
                throw e;
            }
        }
        if (Math.random() < 0.4) {
            try {
                client.get(key);
            } catch (Exception e) {
                logger.error(errMsg("get", key.toStringUtf8(), e));
                if (!handleError(e)) {
                    throw e;
                }
            }
        }
    }

    String errMsg(String method, String key, Exception e) {
        return String.format("encounter error while %s [key=%s] [err=%s]", method, key, e.toString());
    }

    boolean handleError(Exception e) {
        String err = e.getMessage();
        if (err.contains("key_not_in_region") || err.contains("key corrupted")) {
            mutex.lock();
            if (result == null) {
                result = new Result(1, err);
            }
            mutex.unlock();
            return false;
        }
        return true;
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
