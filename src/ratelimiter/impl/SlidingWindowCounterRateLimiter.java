package ratelimiter.impl;

import ratelimiter.RateLimiter;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sliding Window Counter Rate Limiter (Hybrid Approximator).
 *
 * Concept:
 * - Combines Fixed Window low memory (O(1)) with Sliding Log burst protection.
 * - Weights the previous window count based on time elapsed into the current window:
 *     estimatedCount = previousCount * ((windowSize - timeIntoWindow) / windowSize) + currentCount
 * - Industry standard in Cloudflare, Kong, AWS API Gateway.
 *
 * Thread-Safety:
 * - ReentrantLock guards window progression and counter updates.
 */
public class SlidingWindowCounterRateLimiter implements RateLimiter {

    private final int maxRequests;
    private final long windowSizeInMillis;

    private long currentWindowStart;
    private int currentWindowCount;
    private int previousWindowCount;

    private final ReentrantLock lock = new ReentrantLock();

    public SlidingWindowCounterRateLimiter(int maxRequests, long windowSizeInMillis) {
        if (maxRequests <= 0 || windowSizeInMillis <= 0) {
            throw new IllegalArgumentException("Thresholds must be positive.");
        }
        this.maxRequests = maxRequests;
        this.windowSizeInMillis = windowSizeInMillis;
        this.currentWindowStart = System.currentTimeMillis();
        this.currentWindowCount = 0;
        this.previousWindowCount = 0;
    }

    /**
     * Steps:
     * 1. Acquire lock.
     * 2. Advance window if current time exceeds window boundary.
     * 3. Compute weighted request estimate.
     * 4. If estimate + tokens <= maxRequests, add tokens to currentWindowCount and return true.
     * 5. Else return false.
     */
    @Override
    public boolean allowRequest(int tokens) {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Tokens must be > 0");
        }

        long now = System.currentTimeMillis();

        lock.lock();
        try {
            advanceWindow(now);

            double timeIntoCurrentWindow = now - currentWindowStart;
            double previousWindowWeight = Math.max(0.0, (windowSizeInMillis - timeIntoCurrentWindow) / (double) windowSizeInMillis);

            double estimatedCount = (previousWindowCount * previousWindowWeight) + currentWindowCount;

            if (estimatedCount + tokens <= maxRequests) {
                currentWindowCount += tokens;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    private void advanceWindow(long now) {
        long elapsed = now - currentWindowStart;
        if (elapsed >= windowSizeInMillis) {
            long windowsPassed = elapsed / windowSizeInMillis;
            if (windowsPassed == 1) {
                previousWindowCount = currentWindowCount;
            } else {
                previousWindowCount = 0;
            }
            currentWindowCount = 0;
            currentWindowStart += windowsPassed * windowSizeInMillis;
        }
    }

    public double getEstimatedCount() {
        long now = System.currentTimeMillis();
        lock.lock();
        try {
            advanceWindow(now);
            double timeIntoCurrentWindow = now - currentWindowStart;
            double previousWindowWeight = Math.max(0.0, (windowSizeInMillis - timeIntoCurrentWindow) / (double) windowSizeInMillis);
            return (previousWindowCount * previousWindowWeight) + currentWindowCount;
        } finally {
            lock.unlock();
        }
    }
}
