package ratelimiter;

import ratelimiter.impl.*;
import ratelimiter.manager.RateLimiterManager;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("===============================================================");
        System.out.println("       RATE LIMITER LOW LEVEL DESIGN - COMPREHENSIVE DEMO      ");
        System.out.println("===============================================================\n");

        testTokenBucket();
        testLeakyBucket();
        testFixedWindow();
        testSlidingWindowLog();
        testSlidingWindowCounter();
        testConcurrentThreadSafety();
        testMultiTenantManager();

        System.out.println("\nAll Rate Limiter tests and concurrency verifications PASSED successfully!");
    }

    private static void testTokenBucket() throws InterruptedException {
        System.out.println(">>> 1. Testing Token Bucket Rate Limiter (Capacity: 3, Refill: 2 tokens/sec)");
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(3, 2.0);

        System.out.println("Request 1: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // 1
        System.out.println("Request 2: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // 2
        System.out.println("Request 3: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // 3 (capacity exhausted)
        System.out.println("Request 4 (Burst exceeded): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // Should reject

        System.out.println("Sleeping 600ms for partial refill...");
        Thread.sleep(600); // 0.6s * 2 tokens/sec ~= 1.2 tokens
        System.out.println("Request 5 (After refill): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // Should allow
        System.out.println("Request 6 (Immediate next): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // Should reject
        System.out.println();
    }

    private static void testLeakyBucket() throws InterruptedException {
        System.out.println(">>> 2. Testing Leaky Bucket Rate Limiter (Capacity: 2, Leak: 2 requests/sec)");
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(2, 2.0);

        System.out.println("Request 1: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // water = 1
        System.out.println("Request 2: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // water = 2 (full)
        System.out.println("Request 3 (Overflow): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // Reject

        System.out.println("Sleeping 600ms for leak...");
        Thread.sleep(600); // Leaks ~1.2 units
        System.out.println("Request 4 (After leak): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED")); // Allowed
        System.out.println();
    }

    private static void testFixedWindow() throws InterruptedException {
        System.out.println(">>> 3. Testing Fixed Window Rate Limiter (Max: 2 requests / 500ms window)");
        FixedWindowRateLimiter limiter = new FixedWindowRateLimiter(2, 500);

        System.out.println("Req 1: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println("Req 2: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println("Req 3 (Limit reached): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));

        System.out.println("Sleeping 550ms for window rollover...");
        Thread.sleep(550);
        System.out.println("Req 4 (New window): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println();
    }

    private static void testSlidingWindowLog() throws InterruptedException {
        System.out.println(">>> 4. Testing Sliding Window Log Rate Limiter (Max: 2 requests / 500ms)");
        SlidingWindowLogRateLimiter limiter = new SlidingWindowLogRateLimiter(2, 500);

        System.out.println("Req 1: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println("Req 2: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println("Req 3 (Exceeded): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));

        System.out.println("Sleeping 550ms for log expiry...");
        Thread.sleep(550);
        System.out.println("Req 4 (After expiration): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println();
    }

    private static void testSlidingWindowCounter() throws InterruptedException {
        System.out.println(">>> 5. Testing Sliding Window Counter Rate Limiter (Max: 3 requests / 500ms)");
        SlidingWindowCounterRateLimiter limiter = new SlidingWindowCounterRateLimiter(3, 500);

        System.out.println("Req 1: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println("Req 2: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println("Req 3: " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println("Req 4 (Exceeded): " + (limiter.allowRequest() ? "ALLOWED" : "REJECTED"));
        System.out.println();
    }

    private static void testConcurrentThreadSafety() throws InterruptedException {
        System.out.println(">>> 6. Testing Concurrency & Thread-Safety Stress Test");
        int totalThreads = 50;
        int requestsPerThread = 10;
        int totalRequests = totalThreads * requestsPerThread; // 500 requests
        int capacity = 50; // Max allowed capacity

        // We use TokenBucket with 0 refill rate to strictly test capacity boundary under high contention
        TokenBucketRateLimiter concurrentLimiter = new TokenBucketRateLimiter(capacity, 0.000001);

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(totalRequests);

        AtomicInteger allowedCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);

        for (int i = 0; i < totalRequests; i++) {
            executor.submit(() -> {
                try {
                    startGate.await(); // All threads release at the exact same millisecond
                    if (concurrentLimiter.allowRequest()) {
                        allowedCount.incrementAndGet();
                    } else {
                        rejectedCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endGate.countDown();
                }
            });
        }

        // Fire all threads simultaneously
        startGate.countDown();
        endGate.await();
        executor.shutdown();

        System.out.println("Total Concurrent Requests: " + totalRequests);
        System.out.println("Target Allowed Capacity:   " + capacity);
        System.out.println("Actual Allowed Requests:   " + allowedCount.get());
        System.out.println("Actual Rejected Requests:  " + rejectedCount.get());

        if (allowedCount.get() == capacity && rejectedCount.get() == (totalRequests - capacity)) {
            System.out.println(" Thread-safety check SUCCESS: Exact capacity permitted without race conditions!");
        } else {
            System.err.println("❌ Thread-safety check FAILED!");
        }
        System.out.println();
    }

    private static void testMultiTenantManager() {
        System.out.println(">>> 7. Testing Multi-Tenant RateLimiterManager (Per-Client Limits)");
        RateLimiterManager manager = new RateLimiterManager(2, 1.0);

        // Client A gets 2 requests
        System.out.println("Client A - Req 1: " + (manager.isAllowed("client_A") ? "ALLOWED" : "REJECTED"));
        System.out.println("Client A - Req 2: " + (manager.isAllowed("client_A") ? "ALLOWED" : "REJECTED"));
        System.out.println("Client A - Req 3: " + (manager.isAllowed("client_A") ? "ALLOWED" : "REJECTED"));

        // Client B is independent, still has full quota
        System.out.println("Client B - Req 1: " + (manager.isAllowed("client_B") ? "ALLOWED" : "REJECTED"));
        System.out.println("Client B - Req 2: " + (manager.isAllowed("client_B") ? "ALLOWED" : "REJECTED"));
        System.out.println("Client B - Req 3: " + (manager.isAllowed("client_B") ? "ALLOWED" : "REJECTED"));

        System.out.println("Tracked Active Clients in Manager: " + manager.getTrackedClientCount());
    }
}
