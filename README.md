# Thread-Safe Rate Limiter (Low-Level Design in Java)

A complete, production-grade Low-Level Design (LLD) of a **Rate Limiter** implemented in Java. Covers all 5 major rate-limiting algorithms with **strict thread safety**, zero race conditions under high concurrency, comprehensive Javadocs, and a multi-tenant client manager.

---

## 1. System Architecture & LLD Diagrams

GitHub renders all Mermaid diagrams below natively.

### 1.1 Complete UML Class Diagram
Exact reflection of all classes, fields, visibility modifiers, concurrency primitives (`ReentrantLock`, `AtomicInteger`), and relationships:

```mermaid
classDiagram
    direction TB

    class RateLimiter {
        <<interface>>
        +allowRequest() boolean
        +allowRequest(tokens: int) boolean
    }

    class TokenBucketRateLimiter {
        -capacity: long
        -refillRatePerSecond: double
        -availableTokens: double
        -lastRefillTimestampNanos: long
        -lock: ReentrantLock
        +TokenBucketRateLimiter(capacity: long, refillRate: double)
        +allowRequest(tokens: int) boolean
        +getAvailableTokens() double
        -refill() void
    }

    class LeakyBucketRateLimiter {
        -capacity: long
        -leakRatePerSecond: double
        -waterLevel: double
        -lastLeakTimestampNanos: long
        -lock: ReentrantLock
        +LeakyBucketRateLimiter(capacity: long, leakRate: double)
        +allowRequest(tokens: int) boolean
        +getWaterLevel() double
        -leak() void
    }

    class FixedWindowRateLimiter {
        -maxRequests: int
        -windowSizeInMillis: long
        -currentWindowStart: volatile long
        -requestCount: AtomicInteger
        -windowResetLock: Object
        +FixedWindowRateLimiter(maxRequests: int, windowSizeInMillis: long)
        +allowRequest(tokens: int) boolean
        +getRequestCount() int
    }

    class SlidingWindowLogRateLimiter {
        -maxRequests: int
        -windowSizeInMillis: long
        -requestTimestamps: Deque~Long~
        -lock: ReentrantLock
        +SlidingWindowLogRateLimiter(maxRequests: int, windowSizeInMillis: long)
        +allowRequest(tokens: int) boolean
        +getCurrentLogSize() int
    }

    class SlidingWindowCounterRateLimiter {
        -maxRequests: int
        -windowSizeInMillis: long
        -currentWindowStart: long
        -currentWindowCount: int
        -previousWindowCount: int
        -lock: ReentrantLock
        +SlidingWindowCounterRateLimiter(maxRequests: int, windowSizeInMillis: long)
        +allowRequest(tokens: int) boolean
        +getEstimatedCount() double
        -advanceWindow(now: long) void
    }

    class RateLimiterManager {
        -clientLimiters: ConcurrentMap~String, RateLimiter~
        -limiterFactory: Function~String, RateLimiter~
        +RateLimiterManager(capacity: long, refillRate: double)
        +RateLimiterManager(limiterFactory: Function)
        +isAllowed(clientId: String) boolean
        +getTrackedClientCount() int
    }

    RateLimiter <|.. TokenBucketRateLimiter : implements
    RateLimiter <|.. LeakyBucketRateLimiter : implements
    RateLimiter <|.. FixedWindowRateLimiter : implements
    RateLimiter <|.. SlidingWindowLogRateLimiter : implements
    RateLimiter <|.. SlidingWindowCounterRateLimiter : implements

    RateLimiterManager o-- RateLimiter : manages 1..* per client
```

---

### 1.2 Sequence Diagram: Thread-Safe Request Execution
Flow of an incoming client request passing through the multi-tenant manager and rate limiter:

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Manager as RateLimiterManager
    participant Map as ConcurrentHashMap
    participant Limiter as TokenBucketRateLimiter
    participant Lock as ReentrantLock

    Client->>Manager: isAllowed("client_123")
    Manager->>Map: computeIfAbsent("client_123", factory)
    Map-->>Manager: returns client's RateLimiter instance
    
    Manager->>Limiter: allowRequest(1)
    Limiter->>Lock: lock()
    
    rect rgb(240, 248, 255)
        note over Limiter: Critical Section
        Limiter->>Limiter: refill() (Calculate elapsed nanos & add tokens)
        alt availableTokens >= 1
            Limiter->>Limiter: availableTokens -= 1
            Limiter-->>Lock: unlock()
            Limiter-->>Manager: return true (ALLOWED)
            Manager-->>Client: HTTP 200 OK
        else availableTokens < 1
            Limiter-->>Lock: unlock()
            Limiter-->>Manager: return false (REJECTED)
            Manager-->>Client: HTTP 429 Too Many Requests
        end
    end
```

---

### 1.3 Decision Flowcharts

#### A. Token Bucket Algorithm
```mermaid
flowchart TD
    Start([Request Arrives]) --> AcquireLock[Acquire ReentrantLock]
    AcquireLock --> CalcElapsed["Calculate elapsed time = (now - lastRefillTimestamp)"]
    CalcElapsed --> Refill["tokensToAdd = elapsed * refillRate<br/>availableTokens = min(capacity, availableTokens + tokensToAdd)"]
    Refill --> CheckTokens{"availableTokens >= requestedTokens?"}
    CheckTokens -- Yes --> DeductTokens["availableTokens -= requestedTokens"]
    DeductTokens --> UnlockAllow[Release Lock] --> Allowed([Return: ALLOWED])
    CheckTokens -- No --> UnlockReject[Release Lock] --> Rejected([Return: REJECTED])
```

#### B. Sliding Window Counter (Approximation) Algorithm
```mermaid
flowchart TD
    Req([Request Arrives]) --> Lock[Acquire ReentrantLock]
    Lock --> CheckWindow{"(now - currentWindowStart) >= windowSize?"}
    CheckWindow -- Yes --> ShiftWindow["previousCount = currentCount<br/>currentCount = 0<br/>currentWindowStart += windowSize"]
    CheckWindow -- No --> CalcWeight
    ShiftWindow --> CalcWeight
    CalcWeight["weight = (windowSize - elapsedInCurrent) / windowSize<br/>estimatedCount = (previousCount * weight) + currentCount"]
    CalcWeight --> UnderLimit{"estimatedCount + tokens <= maxRequests?"}
    UnderLimit -- Yes --> Increment["currentCount += tokens"]
    Increment --> Unlock1[Release Lock] --> Grant([Return: ALLOWED])
    UnderLimit -- No --> Unlock2[Release Lock] --> Deny([Return: REJECTED])
```

---

## 2. Algorithms Implemented

1. **Token Bucket** (`TokenBucketRateLimiter`)
   - Continuous replenishment based on elapsed time ($O(1)$ memory, supports burst traffic).
   - Guarded by `ReentrantLock` for atomic lazy-refill and permit deduction.

2. **Leaky Bucket** (`LeakyBucketRateLimiter`)
   - Fixed capacity queue that drains at a constant rate ($O(1)$ memory, traffic shaping).
   - Smooths out bursts to protect downstream services.

3. **Fixed Window Counter** (`FixedWindowRateLimiter`)
   - Fixed time slices using non-blocking atomic CAS operations (`AtomicInteger.compareAndSet`).
   - Double-checked locking only on window rollover.

4. **Sliding Window Log** (`SlidingWindowLogRateLimiter`)
   - Precise tracking of request timestamps in a synchronized `Deque<Long>`.
   - 100% boundary accuracy with stale timestamp eviction.

5. **Sliding Window Counter** (`SlidingWindowCounterRateLimiter`)
   - Hybrid approximator combining Fixed Window efficiency ($O(1)$ space) with Sliding Log boundary accuracy.
   - Calculates a weighted estimate based on the current window elapsed ratio.

6. **Multi-Tenant Rate Limiter Manager** (`RateLimiterManager`)
   - Thread-safe per-client rate limiter using `ConcurrentHashMap.computeIfAbsent`.

---

## 3. Comparison Matrix

| Algorithm | Time Complexity | Space Complexity | Handles Bursts? | Boundary Spike Issue? | Best Use Case |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Token Bucket** | $O(1)$ | $O(1)$ | Yes (up to capacity) | No | Web APIs, AWS/Stripe-like public rate limiting |
| **Leaky Bucket** | $O(1)$ | $O(1)$ | No (enforces smooth output) | No | Downstream protection & traffic shaping |
| **Fixed Window** | $O(1)$ | $O(1)$ | No | Yes (up to $2\times$ at edges) | Simple counting, low criticality |
| **Sliding Window Log** | $O(N)$ | $O(N)$ | Partial | No (100% accurate) | Security-critical low-volume operations |
| **Sliding Window Counter** | $O(1)$ | $O(1)$ | Moderate | No (smoothed estimate) | High-throughput distributed API gateways |

---

## 4. Project Structure

```text
rate_limiter_lld/
├── src/
│   └── ratelimiter/
│       ├── RateLimiter.java                         # Core contract
│       ├── Main.java                                # Concurrency tests & execution demo
│       ├── impl/
│       │   ├── TokenBucketRateLimiter.java
│       │   ├── LeakyBucketRateLimiter.java
│       │   ├── FixedWindowRateLimiter.java
│       │   ├── SlidingWindowLogRateLimiter.java
│       │   └── SlidingWindowCounterRateLimiter.java
│       └── manager/
│           └── RateLimiterManager.java              # Per-client management
├── .gitignore
└── README.md
```

---

## 5. How to Build and Run

### Prerequisites
- Java 17+ (JDK 17 or higher)

### Compile
```bash
javac -d bin $(find src -name "*.java")
```

### Run Demo & Concurrency Tests
```bash
java -cp bin ratelimiter.Main
```

### Concurrency Stress Test Results
```text
>>> 6. Testing Concurrency & Thread-Safety Stress Test
Total Concurrent Requests: 500
Target Allowed Capacity:   50
Actual Allowed Requests:   50
Actual Rejected Requests:  450
 Thread-safety check SUCCESS: Exact capacity permitted without race conditions!
```
