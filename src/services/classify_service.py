class ClassifyService:
    @staticmethod
    def classify(message: str, subject: str) -> dict:
        """
        Very simple starter implementation of rule-based classification.
        You are encouraged to review and refine the rules to make them more
        realistic and internally consistent.
        """
        text = f"{subject} {message}".lower()

        high_urgency_keywords = (
            "refund",
            "chargeback",
            "payment",
            "billing",
            "charged",
            "fraud",
            "hack",
            "breach",
            "lawsuit",
            "legal",
            "gdpr",
        )
        medium_urgency_keywords = ("urgent", "broken", "cancel", "late", "error")

        urgency = "low"
        if any(keyword in text for keyword in high_urgency_keywords):
            urgency = "high"
        elif any(keyword in text for keyword in medium_urgency_keywords):
            urgency = "medium"

        sentiment = "neutral"
        negative_keywords = (
            "disappointed",
            "angry",
            "terrible",
            "broken",
            "useless",
            "unacceptable",
        )
        if any(keyword in text for keyword in negative_keywords):
            sentiment = "negative"

        question_mark = "?" in text
        question_keywords = (
            "how ",
            "what ",
            "why ",
            "when ",
            "where ",
            "can you",
            "could you",
        )
        is_question = question_mark or any(
            keyword in text for keyword in question_keywords
        )
        critical_issue = urgency == "high"
        if critical_issue:
            sentiment = "negative"

        requires_action = is_question or critical_issue or sentiment == "negative"

        return {
            "urgency": urgency,
            "sentiment": sentiment,
            "requires_action": requires_action,
        }
