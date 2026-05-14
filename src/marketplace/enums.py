from enum import Enum


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class PrivacyStatus(str, Enum):
    BLOCKED = "blocked"
    REVIEW_REQUIRED = "review_required"
    CLEAR = "clear"
