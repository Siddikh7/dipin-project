from typing import List, Optional
from datetime import datetime
import httpx
import asyncio
from src.db.mongo import get_db
from src.services.classify_service import ClassifyService
from src.services.notify_service import NotifyService
from src.services.rate_limiter import get_rate_limiter
from src.core.logging import logger


class IngestService:
    def __init__(self):
        self.external_api_url = "http://mock-external-api:9000/external/support-tickets"
        self.classify_service = ClassifyService()
        self.notify_service = NotifyService()

    async def run_ingestion(self, tenant_id: str, job_id: Optional[str] = None) -> dict:
        """
        Fetch tickets from the external API and persist them for a tenant.
        The implementation should take into account pagination, duplicate
        handling, ticket classification and any side effects such as
        notifications and logging.
        """
        db = await get_db()

        # ============================================================
        # 🐛 DEBUG TASK D: Race condition
        # Check-then-act pattern: concurrent requests can both pass.
        # ============================================================
        existing_job = await db.ingestion_jobs.find_one(
            {"tenant_id": tenant_id, "status": "running"}
        )

        # 🐛 If a context switch happens here, multiple requests can pass this point.
        await asyncio.sleep(0)  # intentional yield point

        if existing_job:
            return {
                "status": "already_running",
                "job_id": str(existing_job["_id"]),
                "new_ingested": 0,
                "updated": 0,
                "errors": 0,
            }

        # Record ingestion job start
        if job_id:
            from bson import ObjectId

            job_oid = ObjectId(job_id)
            job_doc = await db.ingestion_jobs.find_one({"_id": job_oid})
            if not job_doc:
                job_doc = {
                    "_id": job_oid,
                    "tenant_id": tenant_id,
                    "status": "running",
                    "started_at": datetime.utcnow(),
                    "progress": 0,
                    "total_pages": None,
                    "processed_pages": 0,
                }
                await db.ingestion_jobs.insert_one(job_doc)
        else:
            job_doc = {
                "tenant_id": tenant_id,
                "status": "running",
                "started_at": datetime.utcnow(),
                "progress": 0,
                "total_pages": None,
                "processed_pages": 0,
            }
            result = await db.ingestion_jobs.insert_one(job_doc)
            job_id = str(result.inserted_id)
            from bson import ObjectId

            job_oid = ObjectId(job_id)

        logger.info("Ingestion started tenant=%s job_id=%s", tenant_id, job_id)

        # TODO: implement ingestion behaviour
        # - Handle pagination
        # - Guarantee idempotency (upsert)
        # - Invoke classification service
        # - Invoke notification service for high-urgency tickets
        # - Handle rate limiting (wait on 429 + Retry-After)

        new_ingested = 0
        updated = 0
        errors = 0

        try:

            def _parse_datetime(value: Optional[str]) -> datetime:
                if isinstance(value, datetime):
                    return value
                if isinstance(value, str):
                    normalized = value.replace("Z", "+00:00")
                    try:
                        return datetime.fromisoformat(normalized)
                    except ValueError:
                        return datetime.utcnow()
                return datetime.utcnow()

            page = 1
            page_size = 50
            processed_pages = 0
            total_pages = None
            max_tickets = 100
            processed_tickets = 0
            cancelled = False
            limiter = get_rate_limiter()

            async with httpx.AsyncClient(timeout=10.0) as client:
                while True:
                    job_state = await db.ingestion_jobs.find_one(
                        {"_id": job_oid}, {"status": 1}
                    )
                    if job_state and job_state.get("status") == "cancelled":
                        cancelled = True
                        logger.info(
                            "Ingestion cancelled tenant=%s job_id=%s",
                            tenant_id,
                            job_id,
                        )
                        break

                    params = {"page": page, "page_size": page_size}

                    while True:
                        await limiter.wait_and_acquire()
                        response = await client.get(
                            self.external_api_url, params=params
                        )
                        if response.status_code == 429:
                            retry_after = int(response.headers.get("Retry-After", "1"))
                            logger.info(
                                "Ingestion rate limited for tenant %s (page %s). Retry after %ss",
                                tenant_id,
                                page,
                                retry_after,
                            )
                            await asyncio.sleep(retry_after)
                            continue
                        response.raise_for_status()
                        break

                    payload = response.json()
                    tickets = payload.get("tickets", [])
                    next_page = payload.get("next_page")
                    total_count = payload.get("total_count")

                    if total_count is not None:
                        total_pages = max(1, (total_count + page_size - 1) // page_size)
                        await db.ingestion_jobs.update_one(
                            {"_id": job_oid},
                            {"$set": {"total_pages": total_pages}},
                        )

                    logger.info(
                        "Ingestion progress tenant=%s page=%s total_pages=%s tickets=%s",
                        tenant_id,
                        page,
                        total_pages if total_pages is not None else "?",
                        len(tickets),
                    )

                    for ticket in tickets:
                        if processed_tickets >= max_tickets:
                            break
                        if ticket.get("tenant_id") != tenant_id:
                            continue

                        external_id = ticket.get("id")
                        created_at = _parse_datetime(ticket.get("created_at"))
                        updated_at = _parse_datetime(ticket.get("updated_at"))

                        classification = self.classify_service.classify(
                            ticket.get("message", ""), ticket.get("subject", "")
                        )

                        doc = {
                            "external_id": external_id,
                            "tenant_id": tenant_id,
                            "source": ticket.get("source"),
                            "customer_id": ticket.get("customer_id"),
                            "subject": ticket.get("subject"),
                            "message": ticket.get("message"),
                            "created_at": created_at,
                            "updated_at": updated_at,
                            "status": ticket.get("status"),
                            "urgency": classification.get("urgency"),
                            "sentiment": classification.get("sentiment"),
                            "requires_action": classification.get("requires_action"),
                        }

                        try:
                            result_doc = await db.tickets.update_one(
                                {"tenant_id": tenant_id, "external_id": external_id},
                                {"$set": doc},
                                upsert=True,
                            )

                            if result_doc.upserted_id:
                                new_ingested += 1
                            elif result_doc.modified_count > 0:
                                updated += 1

                            processed_tickets += 1

                            if doc["urgency"] == "high":
                                await self.notify_service.send_notification(
                                    ticket_id=external_id,
                                    tenant_id=tenant_id,
                                    urgency=doc["urgency"],
                                    reason="high_urgency",
                                )
                        except Exception:
                            errors += 1

                    processed_pages += 1
                    progress = 0
                    if total_pages:
                        progress = int((processed_pages / total_pages) * 100)

                    await db.ingestion_jobs.update_one(
                        {"_id": job_oid},
                        {
                            "$set": {
                                "processed_pages": processed_pages,
                                "progress": progress,
                            }
                        },
                    )

                    if not next_page:
                        break

                    if processed_tickets >= max_tickets:
                        break

                    page = next_page

        except Exception as e:
            logger.exception("Ingestion failed tenant=%s job_id=%s", tenant_id, job_id)
            await db.ingestion_logs.insert_one(
                {
                    "tenant_id": tenant_id,
                    "job_id": job_id,
                    "status": "FAILED",
                    "error": str(e),
                    "start_time": job_doc["started_at"],
                    "end_time": datetime.utcnow(),
                    "tickets_processed": new_ingested + updated + errors,
                }
            )
            raise

        if cancelled:
            await db.ingestion_logs.insert_one(
                {
                    "tenant_id": tenant_id,
                    "job_id": job_id,
                    "status": "cancelled",
                    "started_at": job_doc["started_at"],
                    "ended_at": datetime.utcnow(),
                    "new_tickets": new_ingested,
                    "new_ingested": new_ingested,
                    "updated": updated,
                    "errors": errors,
                }
            )
            return {
                "status": "cancelled",
                "job_id": job_id,
                "new_ingested": new_ingested,
                "updated": updated,
                "errors": errors,
            }

        # Log successful completion
        await db.ingestion_jobs.update_one(
            {"_id": job_oid},
            {"$set": {"status": "completed", "ended_at": datetime.utcnow()}},
        )
        await db.ingestion_logs.insert_one(
            {
                "tenant_id": tenant_id,
                "job_id": job_id,
                "status": "SUCCESS",
                "start_time": job_doc["started_at"],
                "end_time": datetime.utcnow(),
                "tickets_processed": new_ingested + updated + errors,
            }
        )

        logger.info(
            "Ingestion completed tenant=%s job_id=%s new=%s updated=%s errors=%s",
            tenant_id,
            job_id,
            new_ingested,
            updated,
            errors,
        )

        return {
            "status": "completed",
            "job_id": job_id,
            "new_ingested": new_ingested,
            "updated": updated,
            "errors": errors,
        }

    async def get_job_status(self, job_id: str) -> Optional[dict]:
        """Retrieve the status of a specific ingestion job."""
        db = await get_db()
        from bson import ObjectId

        job = await db.ingestion_jobs.find_one({"_id": ObjectId(job_id)})
        if not job:
            return None

        return {
            "job_id": job_id,
            "tenant_id": job["tenant_id"],
            "status": job["status"],
            "progress": job.get("progress", 0),
            "total_pages": job.get("total_pages"),
            "processed_pages": job.get("processed_pages", 0),
            "started_at": (
                job["started_at"].isoformat() if job.get("started_at") else None
            ),
            "ended_at": job["ended_at"].isoformat() if job.get("ended_at") else None,
        }

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel an ongoing ingestion job, if it is still running."""
        db = await get_db()
        from bson import ObjectId

        result = await db.ingestion_jobs.update_one(
            {"_id": ObjectId(job_id), "status": "running"},
            {"$set": {"status": "cancelled", "ended_at": datetime.utcnow()}},
        )
        return result.modified_count > 0

    async def get_ingestion_status(self, tenant_id: str) -> Optional[dict]:
        """Get the current ingestion status for a given tenant."""
        db = await get_db()

        job = await db.ingestion_jobs.find_one(
            {"tenant_id": tenant_id, "status": "running"}, sort=[("started_at", -1)]
        )

        if not job:
            return None

        return {
            "job_id": str(job["_id"]),
            "tenant_id": tenant_id,
            "status": job["status"],
            "started_at": (
                job["started_at"].isoformat() if job.get("started_at") else None
            ),
        }
