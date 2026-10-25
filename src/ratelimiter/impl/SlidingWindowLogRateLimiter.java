package ratelimiter.impl;

import ratelimiter.RateLimiter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sliding Window Log Rate Limiter.
 *
 * Concept:
 * - Records timestamps for every accepted request in a double-ended queue.
 * - When a request arrives, evicts all timestamps older than (now - windowSizeInMillis).
 * - Exact 100% accuracy, solves boundary issues completely.
 * - Memory complexity is O(N) where N is number of requests in the window.
 *
 * Thread-Safety:
 * - ReentrantLock synchronizes eviction and insertion on the Deque.
 */
public class SlidingWindowLogRateLimiter implements RateLimiter {

    private final int maxRequests;
    private final long windowSizeInMillis;

    private final Deque<Long> requestTimestamps = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    public SlidingWindowLogRateLimiter(int maxRequests, long windowSizeInMillis) {
        if (maxRequests <= 0 || windowSizeInMillis <= 0) {
            throw new IllegalArgumentException("Thresholds must be positive.");
        }
        this.maxRequests = maxRequests;
        this.windowSizeInMillis = windowSizeInMillis;
    }

    /**
     * Steps:
     * 1. Acquire lock.
     * 2. Evict timestamps strictly older than (now - windowSizeInMillis).
     * 3. If size + tokens <= maxRequests, append 'tokens' instances of 'now' and return true.
     * 4. Else return false.
     */
    @Override
    public boolean allowRequest(int tokens) {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Tokens must be > 0");
        }

        long now = System.currentTimeMillis();
        long windowBoundary = now - windowSizeInMillis;

        lock.lock();
        try {
            while (!requestTimestamps.isEmpty() && requestTimestamps.peekFirst() <= windowBoundary) {
                requestTimestamps.pollFirst();
            }

            if (requestTimestamps.size() + tokens <= maxRequests) {
                for (int i = 0; i < tokens; i++) {
                    requestTimestamps.addLast(now);
                }
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public int getCurrentLogSize() {
        lock.lock();
        try {
            return requestTimestamps.size();
        } finally {
            lock.unlock();
        }
    }
}
