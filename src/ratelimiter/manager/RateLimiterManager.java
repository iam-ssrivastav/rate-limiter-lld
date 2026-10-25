package ratelimiter.manager;

import ratelimiter.RateLimiter;
import ratelimiter.impl.TokenBucketRateLimiter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Multi-client / Multi-tenant Rate Limiter Manager.
 *
 * Thread-Safety:
 * - Uses ConcurrentHashMap.computeIfAbsent to guarantee atomic instantiation per client key,
 *   preventing race conditions when multiple concurrent requests arrive for a new client.
 */
public class RateLimiterManager {

    private final ConcurrentMap<String, RateLimiter> clientLimiters = new ConcurrentHashMap<>();
    private final Function<String, RateLimiter> limiterFactory;

    public RateLimiterManager(long capacity, double refillRatePerSecond) {
        this(clientId -> new TokenBucketRateLimiter(capacity, refillRatePerSecond));
    }

    public RateLimiterManager(Function<String, RateLimiter> limiterFactory) {
        this.limiterFactory = limiterFactory;
    }

    /**
     * Checks if a request for a given client is allowed.
     *
     * @param clientId unique identifier (e.g. user_id, api_key, ip_address)
     * @return true if permitted, false if rate limited.
     */
    public boolean isAllowed(String clientId) {
        RateLimiter limiter = clientLimiters.computeIfAbsent(clientId, limiterFactory);
        return limiter.allowRequest();
    }

    public int getTrackedClientCount() {
        return clientLimiters.size();
    }
}
