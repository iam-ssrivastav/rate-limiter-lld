package ratelimiter.impl;

import ratelimiter.RateLimiter;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Token Bucket Rate Limiter.
 *
 * Concept:
 * - Tokens accumulate at a continuous refillRatePerSecond up to a maximum capacity.
 * - Each request consumes tokens.
 * - Allows bursts up to 'capacity' tokens.
 *
 * Thread-Safety:
 * - Employs a ReentrantLock to guard atomic state updates (availableTokens, lastRefillTimestampNanos).
 * - Refill is calculated lazily on each request invocation based on elapsed nanoseconds,
 *   avoiding the overhead of a continuous background thread.
 */
public class TokenBucketRateLimiter implements RateLimiter {

    private final long capacity;
    private final double refillRatePerSecond;

    private double availableTokens;
    private long lastRefillTimestampNanos;

    private final ReentrantLock lock = new ReentrantLock();

    public TokenBucketRateLimiter(long capacity, double refillRatePerSecond) {
        if (capacity <= 0 || refillRatePerSecond <= 0) {
            throw new IllegalArgumentException("Capacity and refill rate must be positive.");
        }
        this.capacity = capacity;
        this.refillRatePerSecond = refillRatePerSecond;
        this.availableTokens = capacity;
        this.lastRefillTimestampNanos = System.nanoTime();
    }

    /**
     * Attempts to acquire 'tokens'.
     *
     * Steps:
     * 1. Acquire lock.
     * 2. Lazily refill tokens based on time passed.
     * 3. If availableTokens >= requested tokens, deduct and return true.
     * 4. Else return false.
     */
    @Override
    public boolean allowRequest(int tokens) {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Requested tokens must be > 0");
        }

        lock.lock();
        try {
            refill();

            if (availableTokens >= tokens) {
                availableTokens -= tokens;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    private void refill() {
        long now = System.nanoTime();
        double elapsedSeconds = (now - lastRefillTimestampNanos) / 1_000_000_000.0;

        if (elapsedSeconds > 0) {
            double tokensToAdd = elapsedSeconds * refillRatePerSecond;
            availableTokens = Math.min(capacity, availableTokens + tokensToAdd);
            lastRefillTimestampNanos = now;
        }
    }

    public double getAvailableTokens() {
        lock.lock();
        try {
            refill();
            return availableTokens;
        } finally {
            lock.unlock();
        }
    }
}
