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

        match: dict = {"tenant_id": tenant_id, "deleted_at": {"$exists": False}}
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
                "$project": {
                    "status": 1,
                    "urgency": 1,
                    "sentiment": 1,
                    "created_at": 1,
                    "subject": 1,
                    "message": 1,
                    "customer_id": 1,
                }
            },
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
                    "by_status": [
                        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                    ],
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
            {
                "$addFields": {
                    "totals_doc": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$totals", 0]},
                            {"total": 0, "high_count": 0, "negative_count": 0},
                        ]
                    }
                }
            },
            {
                "$project": {
                    "total_tickets": "$totals_doc.total",
                    "by_status": {
                        "$cond": [
                            {"$gt": [{"$size": "$by_status"}, 0]},
                            {
                                "$arrayToObject": {
                                    "$map": {
                                        "input": "$by_status",
                                        "as": "item",
                                        "in": ["$$item._id", "$$item.count"],
                                    }
                                }
                            },
                            {},
                        ]
                    },
                    "urgency_high_ratio": {
                        "$cond": [
                            {"$gt": ["$totals_doc.total", 0]},
                            {
                                "$divide": [
                                    "$totals_doc.high_count",
                                    "$totals_doc.total",
                                ]
                            },
                            0.0,
                        ]
                    },
                    "negative_sentiment_ratio": {
                        "$cond": [
                            {"$gt": ["$totals_doc.total", 0]},
                            {
                                "$divide": [
                                    "$totals_doc.negative_count",
                                    "$totals_doc.total",
                                ]
                            },
                            0.0,
                        ]
                    },
                    "hourly_trend": {
                        "$map": {
                            "input": "$hourly_trend",
                            "as": "item",
                            "in": {"hour": "$$item._id", "count": "$$item.count"},
                        }
                    },
                    "top_keywords": {
                        "$map": {
                            "input": "$top_keywords",
                            "as": "item",
                            "in": "$$item._id",
                        }
                    },
                    "at_risk_customers": {
                        "$map": {
                            "input": "$at_risk_customers",
                            "as": "item",
                            "in": {
                                "customer_id": "$$item._id",
                                "count": "$$item.count",
                            },
                        }
                    },
                }
            },
        ]

        agg = await db.tickets.aggregate(pipeline, allowDiskUse=True).to_list(length=1)
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

        return agg[0]
