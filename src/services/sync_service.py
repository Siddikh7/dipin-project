"""
Task 12: Data synchronization service.

Responsible for synchronizing ticket data with the external API.

Requirements:
1. Use the ticket `updated_at` field to update only tickets that have changed.
2. Apply soft delete (`deleted_at` field) for tickets deleted in the external system.
3. Record change history in the `ticket_history` collection.
"""

from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from src.db.mongo import get_db


class SyncService:
    """
    Data synchronization service.
    """

    HISTORY_COLLECTION = "ticket_history"

    async def sync_ticket(self, external_ticket: dict, tenant_id: str) -> dict:
        """
        Synchronize a single ticket.

        Args:
            external_ticket: Ticket payload from the external API.
            tenant_id: Tenant identifier.

        Returns:
            {
                "action": "created" | "updated" | "unchanged",
                "ticket_id": str,
                "changes": List[str]  # list of changed fields
            }

        TODO: Implement:
        - Look up the existing ticket (by tenant_id + external_id).
        - Compare updated_at to decide whether an update is needed.
        - If changed, update the ticket and record history.
        """
        db = await get_db()

        def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    return value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc)
            if isinstance(value, str):
                normalized = value.replace("Z", "+00:00")
                try:
                    parsed = datetime.fromisoformat(normalized)
                    if parsed.tzinfo is None:
                        return parsed.replace(tzinfo=timezone.utc)
                    return parsed.astimezone(timezone.utc)
                except ValueError:
                    return None
            return None

        def _now_utc() -> datetime:
            return datetime.now(timezone.utc)

        external_id = external_ticket.get("id") or external_ticket.get("external_id")
        updated_at = _parse_datetime(external_ticket.get("updated_at"))
        created_at = _parse_datetime(external_ticket.get("created_at"))

        existing = await db.tickets.find_one(
            {"tenant_id": tenant_id, "external_id": external_id}
        )

        if not existing:
            doc = {
                "external_id": external_id,
                "tenant_id": tenant_id,
                "subject": external_ticket.get("subject"),
                "message": external_ticket.get("message"),
                "status": external_ticket.get("status"),
                "created_at": created_at or _now_utc(),
                "updated_at": updated_at or created_at or _now_utc(),
            }
            await db.tickets.insert_one(doc)
            await self.record_history(external_id, tenant_id, "created")
            return {"action": "created", "ticket_id": external_id, "changes": []}

        existing_updated_at = _parse_datetime(existing.get("updated_at"))
        if updated_at and existing_updated_at:
            if updated_at.timestamp() <= existing_updated_at.timestamp():
                return {"action": "unchanged", "ticket_id": external_id, "changes": []}

        update_doc = {
            "subject": external_ticket.get("subject", existing.get("subject")),
            "message": external_ticket.get("message", existing.get("message")),
            "status": external_ticket.get("status", existing.get("status")),
            "updated_at": updated_at or _now_utc(),
        }

        existing_for_compare = dict(existing)
        existing_for_compare["updated_at"] = existing_updated_at
        update_doc["updated_at"] = _parse_datetime(update_doc["updated_at"])

        changes = self.compute_changes(
            existing_for_compare, update_doc, list(update_doc.keys())
        )
        if not changes:
            return {"action": "unchanged", "ticket_id": external_id, "changes": []}

        await db.tickets.update_one(
            {"tenant_id": tenant_id, "external_id": external_id},
            {"$set": update_doc},
        )
        await self.record_history(external_id, tenant_id, "updated", changes)

        return {
            "action": "updated",
            "ticket_id": external_id,
            "changes": list(changes.keys()),
        }

    async def mark_deleted(self, tenant_id: str, external_ids: List[str]) -> int:
        """
        Handle tickets that were deleted in the external system (soft delete).

        Args:
            tenant_id: Tenant identifier.
            external_ids: List of external ticket IDs that have been deleted.

        Returns:
            Number of tickets that were soft-deleted.

        TODO: Implement:
        - Set the `deleted_at` field.
        - Record a history entry.
        """
        if not external_ids:
            return 0

        db = await get_db()
        now = datetime.now(timezone.utc)

        cursor = db.tickets.find(
            {
                "tenant_id": tenant_id,
                "external_id": {"$in": external_ids},
                "deleted_at": {"$exists": False},
            }
        )
        docs = await cursor.to_list(length=len(external_ids))

        if not docs:
            return 0

        await db.tickets.update_many(
            {
                "tenant_id": tenant_id,
                "external_id": {"$in": external_ids},
                "deleted_at": {"$exists": False},
            },
            {"$set": {"deleted_at": now}},
        )

        for doc in docs:
            await self.record_history(doc.get("external_id"), tenant_id, "deleted")

        return len(docs)

    async def detect_deleted_tickets(
        self, tenant_id: str, external_ids: List[str]
    ) -> List[str]:
        """
        Detect tickets that appear to have been deleted externally.

        Finds tickets that exist in our DB but are missing from `external_ids`.

        Args:
            tenant_id: Tenant identifier.
            external_ids: Complete list of ticket IDs from the external API.

        Returns:
            List of external IDs that are presumed deleted.

        TODO: Implement this query.
        """
        db = await get_db()
        query = {"tenant_id": tenant_id}
        if external_ids:
            query["external_id"] = {"$nin": external_ids}

        cursor = db.tickets.find(query, {"external_id": 1})
        docs = await cursor.to_list(length=10000)
        return [doc.get("external_id") for doc in docs if doc.get("external_id")]

    async def record_history(
        self,
        ticket_id: str,
        tenant_id: str,
        action: str,
        changes: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Record a change history entry.

        Args:
            ticket_id: Ticket identifier.
            tenant_id: Tenant identifier.
            action: "created" | "updated" | "deleted".
            changes: Change details (field -> {old: ..., new: ...}).

        Returns:
            ID of the created history document.
        """
        db = await get_db()

        history_doc = {
            "ticket_id": ticket_id,
            "tenant_id": tenant_id,
            "action": action,
            "changes": changes or {},
            "recorded_at": datetime.now(timezone.utc),
        }

        result = await db[self.HISTORY_COLLECTION].insert_one(history_doc)
        return str(result.inserted_id)

    async def get_ticket_history(
        self, ticket_id: str, tenant_id: str, limit: int = 50
    ) -> List[dict]:
        """
        Retrieve ticket change history.

        Args:
            ticket_id: Ticket identifier.
            tenant_id: Tenant identifier.
            limit: Maximum number of records to return.

        Returns:
            List of history entries in reverse chronological order.
        """
        db = await get_db()

        cursor = (
            db[self.HISTORY_COLLECTION]
            .find({"ticket_id": ticket_id, "tenant_id": tenant_id})
            .sort("recorded_at", -1)
            .limit(limit)
        )
        history = await cursor.to_list(length=limit)
        for item in history:
            if "_id" in item:
                item["_id"] = str(item["_id"])
        return history

    def compute_changes(
        self, old_doc: dict, new_doc: dict, fields: List[str]
    ) -> Dict[str, Any]:
        """
        Compute field-level differences between two documents.

        Args:
            old_doc: Previous version of the document.
            new_doc: New version of the document.
            fields: List of fields to compare.

        Returns:
            A mapping of changed fields to their before/after values:
            {
                "field_name": {"old": ..., "new": ...},
                ...
            }
        """
        changes = {}

        for field in fields:
            old_value = old_doc.get(field)
            new_value = new_doc.get(field)

            if old_value != new_value:
                changes[field] = {"old": old_value, "new": new_value}

        return changes
