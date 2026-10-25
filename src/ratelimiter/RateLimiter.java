package ratelimiter;

/**
 * Common contract for all rate limiter implementations.
 */
public interface RateLimiter {

    /**
     * Attempts to acquire 1 token/permit for a single request.
     *
     * @return true if permitted, false if rate limited.
     */
    default boolean allowRequest() {
        return allowRequest(1);
    }

    /**
     * Attempts to acquire a specified number of tokens/permits.
     * Useful for batch or weighted operations.
     *
     * @param tokens Number of permits requested.
     * @return true if permitted, false if rate limited.
     */
    boolean allowRequest(int tokens);
}
