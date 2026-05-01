"""
Exponential Backoff with Jitter — retry delay strategy for producers.

When a distributed system is under load, the worst thing a client can
do is retry immediately. If 1000 producers all get rejected and all
retry at exactly the same time, they create a "retry storm" that's
even worse than the original load spike. This is called the
"thundering herd" problem.

Exponential backoff solves this by making each successive retry wait
longer:
    Attempt 1: wait 1s
    Attempt 2: wait 2s
    Attempt 3: wait 4s
    Attempt 4: wait 8s
    ...and so on, doubling each time up to a maximum cap.

But pure exponential backoff still has a problem: if all 1000 clients
start at the same time, they'll all wait 1s, then all retry together
at t=1s, all wait 2s, all retry at t=3s, etc. They stay synchronized.

Adding JITTER (randomness) breaks this synchronization. Instead of
waiting exactly 4s, a client waits somewhere between 0 and 4s. Now
the retries spread out over time instead of arriving in bursts.

This is the exact same strategy used by AWS SDKs, gRPC, and HTTP
clients. Amazon published a famous blog post about it:
"Exponential Backoff And Jitter" (aws.amazon.com/blogs/architecture).

Two common jitter strategies:
    - "Full jitter":     wait = random(0, min(cap, base * 2^attempt))
    - "Decorrelated":    wait = random(base, previous_wait * 3)

We implement full jitter here as it provides the best spread.
"""

import random
import logging

logger = logging.getLogger(__name__)


class ExponentialBackoff:
    """
    Calculates retry delays using exponential backoff with full jitter.

    Usage:
        backoff = ExponentialBackoff()

        # In a retry loop:
        delay = backoff.next_wait()
        await asyncio.sleep(delay)

        # Or with a server-suggested retry_after hint:
        delay = backoff.next_wait(retry_after=response["retry_after_seconds"])

        # Reset after a successful operation:
        backoff.reset()

    Attributes:
        base_delay:   Starting delay in seconds (default: 1.0)
        max_delay:    Maximum delay cap in seconds (default: 60.0)
        multiplier:   Factor to multiply delay by on each attempt (default: 2.0)
        jitter:       Whether to add randomness (default: True)
        max_attempts: Maximum number of retries before giving up (default: 10)
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: bool = True,
        max_attempts: int = 10,
    ):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
        self.max_attempts = max_attempts
        self._attempt: int = 0

    @property
    def attempt(self) -> int:
        """Current attempt number (0 = first attempt hasn't happened yet)."""
        return self._attempt

    @property
    def exhausted(self) -> bool:
        """True if we've exceeded the maximum number of retry attempts."""
        return self._attempt >= self.max_attempts

    def next_wait(self, retry_after: float | None = None) -> float:
        """
        Calculate the next wait duration and advance the attempt counter.

        If the server provides a `retry_after` hint (like an HTTP 429
        Retry-After header), we use that as a floor — we'll wait at
        least that long, but our exponential calculation might be higher.

        Args:
            retry_after: Optional server-suggested minimum wait time in
                         seconds. The actual wait will be at least this.

        Returns:
            The number of seconds to wait before the next retry.

        Raises:
            RuntimeError: If max_attempts has been exceeded.
        """
        if self.exhausted:
            raise RuntimeError(
                f"Exponential backoff exhausted after {self.max_attempts} attempts. "
                f"Giving up."
            )

        # Calculate the exponential delay: base * multiplier^attempt
        # For default values: 1, 2, 4, 8, 16, 32, 60, 60, 60...
        exp_delay = self.base_delay * (self.multiplier ** self._attempt)

        # Cap at max_delay to prevent absurdly long waits
        capped_delay = min(exp_delay, self.max_delay)

        # Apply full jitter: pick a random value between 0 and capped_delay
        # This spreads out retries across the entire window
        if self.jitter:
            delay = random.uniform(0, capped_delay)
        else:
            delay = capped_delay

        # If the server said "retry after X seconds", respect that as a floor
        if retry_after is not None:
            delay = max(delay, retry_after)

        self._attempt += 1

        logger.debug(
            f"Backoff attempt #{self._attempt}: waiting {delay:.2f}s "
            f"(exp={exp_delay:.2f}s, cap={capped_delay:.2f}s, "
            f"retry_after={retry_after})"
        )

        return delay

    def reset(self):
        """
        Reset the attempt counter after a successful operation.

        Call this after a successful produce so that the next failure
        starts the backoff sequence from the beginning.
        """
        if self._attempt > 0:
            logger.debug(f"Backoff reset (was at attempt #{self._attempt})")
        self._attempt = 0

    def __repr__(self) -> str:
        return (
            f"ExponentialBackoff(base={self.base_delay}, max={self.max_delay}, "
            f"multiplier={self.multiplier}, attempt={self._attempt}/{self.max_attempts})"
        )
