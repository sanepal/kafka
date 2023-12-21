package org.apache.kafka.clients.consumer.internals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class BugReplicator {

    private final Object cleanupThreadWaiter = new Object();
    private final Object joinRequestWaiter = new Object();
    private long triggerFileLastModified = 0L;
    private AtomicInteger counter = new AtomicInteger(0);
    private Thread testThread = null;
    private static final Logger LOG = LoggerFactory.getLogger(BugReplicator.class);

    private BugReplicator() { }

    public static BugReplicator instance = new BugReplicator();

    public void waitOnCleanupThread() throws InterruptedException {
        if (counter.get() != 0) {
            return;
        }
        File file = Paths.get("/etc/debug2").toFile();
        if (!file.exists() || file.lastModified() == triggerFileLastModified) {
            return;
        }
        if (counter.getAndIncrement() != 0) {
            return;
        }
        testThread = Thread.currentThread();
        triggerFileLastModified = file.lastModified();
        LOG.info("Notifying cleanup to run and waiting on it to finish");
        synchronized (joinRequestWaiter) {
            joinRequestWaiter.notifyAll();
        }
        synchronized (cleanupThreadWaiter) {
            cleanupThreadWaiter.wait();
        }
        LOG.info("Done waiting on cleanup");
    }

    public void waitOnJoinRequest() throws InterruptedException {
        synchronized (joinRequestWaiter) {
            if (testThread == null) {
                joinRequestWaiter.wait();
            }
        }
    }

    public void notifyCleanupComplete() {
        synchronized (cleanupThreadWaiter) {
            cleanupThreadWaiter.notifyAll();
        }
    }

    public boolean shouldRejoinOnSync() {
        if (Thread.currentThread().equals(testThread)) {
            LOG.info("Cleaning up state");
            // Reset state
            testThread = null;
            counter = new AtomicInteger(0);
            return true;
        }
        return false;
    }
}
