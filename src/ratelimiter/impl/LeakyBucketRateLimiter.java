package ratelimiter.impl;

import ratelimiter.RateLimiter;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Leaky Bucket Rate Limiter.
 *
 * Concept:
 * - Represents a bucket that holds incoming requests (water level) up to a capacity.
 * - Leaks (processes) at a constant rate (leakRatePerSecond).
 * - Smooths out bursty traffic, preventing sudden surges on downstream services.
 *
 * Thread-Safety:
 * - ReentrantLock ensures atomic calculation of water leakage and bucket occupancy.
 */
public class LeakyBucketRateLimiter implements RateLimiter {

    private final long capacity;
    private final double leakRatePerSecond;

    private double waterLevel; // Current volume of pending requests
    private long lastLeakTimestampNanos;

    private final ReentrantLock lock = new ReentrantLock();

    public LeakyBucketRateLimiter(long capacity, double leakRatePerSecond) {
        if (capacity <= 0 || leakRatePerSecond <= 0) {
            throw new IllegalArgumentException("Capacity and leak rate must be positive.");
        }
        this.capacity = capacity;
        this.leakRatePerSecond = leakRatePerSecond;
        this.waterLevel = 0.0;
        this.lastLeakTimestampNanos = System.nanoTime();
    }

    /**
     * Steps:
     * 1. Acquire lock.
     * 2. Leak water based on elapsed time.
     * 3. If waterLevel + tokens <= capacity: add tokens to waterLevel and return true.
     * 4. Else: bucket overflows, return false (rejected).
     */
    @Override
    public boolean allowRequest(int tokens) {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Tokens must be > 0");
        }

        lock.lock();
        try {
            leak();

            if (waterLevel + tokens <= capacity) {
                waterLevel += tokens;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    private void leak() {
        long now = System.nanoTime();
        double elapsedSeconds = (now - lastLeakTimestampNanos) / 1_000_000_000.0;

        if (elapsedSeconds > 0) {
            double leakedWater = elapsedSeconds * leakRatePerSecond;
            waterLevel = Math.max(0.0, waterLevel - leakedWater);
            lastLeakTimestampNanos = now;
        }
    }

    public double getWaterLevel() {
        lock.lock();
        try {
            leak();
            return waterLevel;
        } finally {
            lock.unlock();
        }
    }
}
