"""
Seed default tier configuration if the table is empty.
"""
from datetime import date
from . import db
from .models import TierConfig

TIERS = [
    {
        "tier_name": "Bronze",
        "tier_name_he": "ארד",
        "min_gci": 0.0,
        "max_gci": 200_000.0,
        "agent_pct": 50.0,
        "office_pct": 50.0,
        "badge_color": "#CD7F32",
        "badge_icon": "circle",
        "sort_order": 1,
    },
    {
        "tier_name": "Silver",
        "tier_name_he": "כסף",
        "min_gci": 200_000.0,
        "max_gci": 350_000.0,
        "agent_pct": 60.0,
        "office_pct": 40.0,
        "badge_color": "#C0C0C0",
        "badge_icon": "star",
        "sort_order": 2,
    },
    {
        "tier_name": "Gold",
        "tier_name_he": "זהב",
        "min_gci": 350_000.0,
        "max_gci": 600_000.0,
        "agent_pct": 70.0,
        "office_pct": 30.0,
        "badge_color": "#FFD700",
        "badge_icon": "star-filled",
        "sort_order": 3,
    },
    {
        "tier_name": "Platinum",
        "tier_name_he": "פלטינום",
        "min_gci": 600_000.0,
        "max_gci": 1_000_000.0,
        "agent_pct": 80.0,
        "office_pct": 20.0,
        "badge_color": "#E5E4E2",
        "badge_icon": "diamond",
        "sort_order": 4,
    },
    {
        "tier_name": "Diamond",
        "tier_name_he": "יהלום",
        "min_gci": 1_000_000.0,
        "max_gci": None,       # unlimited
        "agent_pct": 85.0,
        "office_pct": 15.0,
        "badge_color": "#B9F2FF",
        "badge_icon": "gem",
        "sort_order": 5,
    },
]


def seed_tiers_if_empty():
    if TierConfig.query.count() == 0:
        for t in TIERS:
            db.session.add(TierConfig(
                tier_name=t["tier_name"],
                tier_name_he=t["tier_name_he"],
                min_gci=t["min_gci"],
                max_gci=t["max_gci"],
                agent_pct=t["agent_pct"],
                office_pct=t["office_pct"],
                badge_color=t["badge_color"],
                badge_icon=t["badge_icon"],
                sort_order=t["sort_order"],
                is_active=True,
                effective_from=date(2024, 1, 1),
            ))
        db.session.commit()
