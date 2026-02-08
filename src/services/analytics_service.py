from datetime import datetime, timedelta
from src.db.mongo import get_db


class AnalyticsService:
    async def get_tenant_stats(
        self,
        tenant_id: str,
        from_date: datetime,
        to_date: datetime,
    ) -> dict:
        """
        Compute analytics for a tenant within a date range.
        Focus on letting the database do the heavy lifting rather than
        bringing large result sets into application memory.
        """
        db = await get_db()

        match: dict = {"tenant_id": tenant_id}
        if from_date and to_date:
            match["created_at"] = {"$gte": from_date, "$lte": to_date}
        elif from_date:
            match["created_at"] = {"$gte": from_date}
        elif to_date:
            match["created_at"] = {"$lte": to_date}

        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)

        pipeline = [
            {"$match": match},
            {
                "$facet": {
                    "totals": [
                        {
                            "$group": {
                                "_id": None,
                                "total": {"$sum": 1},
                                "high_count": {
                                    "$sum": {
                                        "$cond": [
                                            {"$eq": ["$urgency", "high"]},
                                            1,
                                            0,
                                        ]
                                    }
                                },
                                "negative_count": {
                                    "$sum": {
                                        "$cond": [
                                            {"$eq": ["$sentiment", "negative"]},
                                            1,
                                            0,
                                        ]
                                    }
                                },
                            }
                        }
                    ],
                    "by_status": [{"$group": {"_id": "$status", "count": {"$sum": 1}}}],
                    "hourly_trend": [
                        {"$match": {"created_at": {"$gte": last_24h}}},
                        {
                            "$group": {
                                "_id": {
                                    "$dateTrunc": {
                                        "date": "$created_at",
                                        "unit": "hour",
                                    }
                                },
                                "count": {"$sum": 1},
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ],
                    "top_keywords": [
                        {
                            "$project": {
                                "text": {
                                    "$concat": [
                                        {"$ifNull": ["$subject", ""]},
                                        " ",
                                        {"$ifNull": ["$message", ""]},
                                    ]
                                }
                            }
                        },
                        {
                            "$project": {
                                "words": {"$split": [{"$toLower": "$text"}, " "]}
                            }
                        },
                        {"$unwind": "$words"},
                        {"$match": {"words": {"$nin": ["", " "]}}},
                        {"$group": {"_id": "$words", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 10},
                    ],
                    "at_risk_customers": [
                        {
                            "$match": {
                                "$or": [
                                    {"urgency": "high"},
                                    {"sentiment": "negative"},
                                ]
                            }
                        },
                        {
                            "$group": {
                                "_id": "$customer_id",
                                "count": {"$sum": 1},
                            }
                        },
                        {"$sort": {"count": -1}},
                        {"$limit": 5},
                    ],
                }
            },
        ]

        agg = await db.tickets.aggregate(pipeline).to_list(length=1)
        if not agg:
            return {
                "total_tickets": 0,
                "by_status": {},
                "urgency_high_ratio": 0.0,
                "negative_sentiment_ratio": 0.0,
                "hourly_trend": [],
                "top_keywords": [],
                "at_risk_customers": [],
            }

        data = agg[0]
        totals = data.get("totals", [])
        total = totals[0].get("total", 0) if totals else 0
        high_count = totals[0].get("high_count", 0) if totals else 0
        negative_count = totals[0].get("negative_count", 0) if totals else 0

        by_status = {item["_id"]: item["count"] for item in data.get("by_status", [])}
        hourly_trend = [
            {"hour": item["_id"], "count": item["count"]}
            for item in data.get("hourly_trend", [])
        ]
        top_keywords = [item["_id"] for item in data.get("top_keywords", [])]
        at_risk_customers = [
            {"customer_id": item["_id"], "count": item["count"]}
            for item in data.get("at_risk_customers", [])
        ]

        return {
            "total_tickets": total,
            "by_status": by_status,
            "urgency_high_ratio": (high_count / total) if total else 0.0,
            "negative_sentiment_ratio": (negative_count / total) if total else 0.0,
            "hourly_trend": hourly_trend,
            "top_keywords": top_keywords,
            "at_risk_customers": at_risk_customers,
        }
