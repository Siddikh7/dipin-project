import httpx
import asyncio
import random
from src.core.logging import logger
from src.services.circuit_breaker import get_circuit_breaker, CircuitBreakerOpenError


class NotifyService:
    def __init__(self):
        self.notify_url = "http://mock-external-api:9000/notify"

    async def send_notification(
        self, ticket_id: str, tenant_id: str, urgency: str, reason: str
    ):
        """
        Sends a notification to the external service.

        This should include some form of retry/backoff and must not block
        normal request handling. External retry helper libraries should not
        be required for this exercise.
        """
        payload = {
            "ticket_id": ticket_id,
            "tenant_id": tenant_id,
            "urgency": urgency,
            "reason": reason,
        }

        cb = get_circuit_breaker("notify")

        async def _send_with_retry() -> None:
            max_attempts = 3
            backoff_seconds = 0.5

            for attempt in range(1, max_attempts + 1):
                try:

                    async def _do_post() -> None:
                        async with httpx.AsyncClient(timeout=2.0) as client:
                            response = await client.post(self.notify_url, json=payload)
                            if response.status_code >= 500:
                                raise RuntimeError("notify_failed")

                    await cb.call(_do_post)
                    return
                except CircuitBreakerOpenError:
                    logger.warning(
                        "Notification circuit open, skipping ticket_id=%s",
                        ticket_id,
                    )
                    return
                except Exception:
                    logger.exception(
                        "Notification attempt failed (attempt=%s)", attempt
                    )

                await asyncio.sleep(backoff_seconds)
                backoff_seconds *= 2

            logger.error("Notification failed after retries ticket_id=%s", ticket_id)

        asyncio.create_task(_send_with_retry())
