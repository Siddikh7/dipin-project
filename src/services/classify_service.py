class ClassifyService:
    @staticmethod
    def classify(message: str, subject: str) -> dict:
        """
        Very simple starter implementation of rule-based classification.
        You are encouraged to review and refine the rules to make them more
        realistic and internally consistent.
        """
        text = f"{subject} {message}".lower()

        urgency = "low"
        if any(keyword in text for keyword in ("lawsuit", "gdpr", "urgent")):
            urgency = "high"
        elif any(keyword in text for keyword in ("refund", "broken")):
            urgency = "medium"

        sentiment = "neutral"
        if any(keyword in text for keyword in ("angry", "broken")):
            sentiment = "negative"

        requires_action = urgency in ("high", "medium") or sentiment == "negative"

        return {
            "urgency": urgency,
            "sentiment": sentiment,
            "requires_action": requires_action,
        }
