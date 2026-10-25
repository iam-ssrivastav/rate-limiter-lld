package ratelimiter.impl;

import ratelimiter.RateLimiter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fixed Window Rate Limiter.
 *
 * Concept:
 * - Time is sliced into fixed discrete windows (e.g., 1000ms).
 * - A counter tracks requests within the current window.
 * - Resets on window expiration.
 *
 * Caveat:
 * - Prone to boundary bursts (up to 2x maxRequests across window borders).
 *
 * Thread-Safety:
 * - Synchronized block for thread-safe window transition.
 * - AtomicInteger CAS (Compare-And-Set) loop for lock-free increments during the window.
 */
public class FixedWindowRateLimiter implements RateLimiter {

    private final int maxRequests;
    private final long windowSizeInMillis;

    private volatile long currentWindowStart;
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final Object windowResetLock = new Object();

    public FixedWindowRateLimiter(int maxRequests, long windowSizeInMillis) {
        if (maxRequests <= 0 || windowSizeInMillis <= 0) {
            throw new IllegalArgumentException("Thresholds must be positive.");
        }
        this.maxRequests = maxRequests;
        this.windowSizeInMillis = windowSizeInMillis;
        this.currentWindowStart = System.currentTimeMillis();
    }

    /**
     * Steps:
     * 1. Check if the window elapsed.
     * 2. If elapsed, synchronize and reset start time and requestCount.
     * 3. Use CAS loop on AtomicInteger to atomically check limit and increment.
     */
    @Override
    public boolean allowRequest(int tokens) {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Tokens must be > 0");
        }

        long now = System.currentTimeMillis();

        if (now - currentWindowStart >= windowSizeInMillis) {
            synchronized (windowResetLock) {
                if (now - currentWindowStart >= windowSizeInMillis) {
                    currentWindowStart = now;
                    requestCount.set(0);
                }
            }
        }

        while (true) {
            int current = requestCount.get();
            if (current + tokens > maxRequests) {
                return false;
            }
            if (requestCount.compareAndSet(current, current + tokens)) {
                return true;
            }
        }
    }

    public int getRequestCount() {
        return requestCount.get();
    }
}
