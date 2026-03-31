"""
All REST API endpoints for the compensation module.
Mounted at /comp via the compensation_bp Blueprint.

Admin endpoints require the X-Admin-Key header matching the COMP_ADMIN_KEY env var.
"""
from datetime import date, datetime
from flask import request, jsonify

from . import compensation_bp, db, ADMIN_KEY
from .models import Agent, Transaction, TierConfig, TrainerOverride
from .calculator import (
    calculate_commission,
    simulate_earnings,
    get_agent_dashboard,
    _get_fiscal_year,
    _ytd_gci,
    _active_tiers,
    _tier_for_gci,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json(data, status=200):
    resp = jsonify(data)
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp, status


def _require_admin():
    if ADMIN_KEY and request.headers.get("X-Admin-Key") != ADMIN_KEY:
        return _json({"error": "Unauthorised"}, 401)
    return None


def _parse_date(s):
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@compensation_bp.get("/health")
def comp_health():
    tier_count = TierConfig.query.filter_by(is_active=True).count()
    agent_count = Agent.query.filter_by(is_active=True).count()
    return _json({"status": "ok", "tier_count": tier_count, "agent_count": agent_count})


@compensation_bp.delete("/agents/all")
def delete_all_agents():
    """Admin: wipe all agents + transactions (for re-sync). Requires X-Admin-Key."""
    err = _require_admin()
    if err:
        return err
    Transaction.query.delete()
    TrainerOverride.query.delete()
    Agent.query.delete()
    db.session.commit()
    return _json({"deleted": True})


# ---------------------------------------------------------------------------
# Tier configuration (admin)
# ---------------------------------------------------------------------------

@compensation_bp.get("/tiers")
def list_tiers():
    tiers = TierConfig.query.order_by(TierConfig.sort_order.asc()).all()
    return _json({"tiers": [t.to_dict() for t in tiers]})


@compensation_bp.post("/tiers")
def create_tier():
    err = _require_admin()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    required = ("tier_name", "min_gci", "agent_pct", "office_pct")
    for f in required:
        if f not in data:
            return _json({"error": f"Missing field: {f}"}, 400)

    tier = TierConfig(
        tier_name=data["tier_name"],
        tier_name_he=data.get("tier_name_he"),
        min_gci=float(data["min_gci"]),
        max_gci=float(data["max_gci"]) if data.get("max_gci") is not None else None,
        agent_pct=float(data["agent_pct"]),
        office_pct=float(data["office_pct"]),
        badge_color=data.get("badge_color"),
        badge_icon=data.get("badge_icon"),
        sort_order=int(data.get("sort_order", 99)),
        is_active=data.get("is_active", True),
        effective_from=_parse_date(data.get("effective_from")) or date(2024, 1, 1),
    )
    db.session.add(tier)
    db.session.commit()
    return _json({"tier": tier.to_dict(), "message": "Tier created"}, 201)


@compensation_bp.put("/tiers/<int:tier_id>")
def update_tier(tier_id):
    err = _require_admin()
    if err:
        return err
    tier = TierConfig.query.get_or_404(tier_id)
    data = request.get_json(silent=True) or {}
    for field, cast in [("tier_name", str), ("tier_name_he", str), ("min_gci", float),
                         ("agent_pct", float), ("office_pct", float), ("badge_color", str),
                         ("badge_icon", str), ("sort_order", int), ("is_active", bool)]:
        if field in data:
            setattr(tier, field, cast(data[field]))
    if "max_gci" in data:
        tier.max_gci = float(data["max_gci"]) if data["max_gci"] is not None else None
    db.session.commit()
    return _json({"tier": tier.to_dict(), "message": "Tier updated"})


# ---------------------------------------------------------------------------
# Agent management
# ---------------------------------------------------------------------------

@compensation_bp.post("/agents")
def create_agent():
    data = request.get_json(silent=True) or {}
    if not data.get("name_he"):
        return _json({"error": "name_he is required"}, 400)
    agent = Agent(
        name_he=data["name_he"],
        name_en=data.get("name_en"),
        email=data.get("email"),
        phone=data.get("phone"),
        license_number=data.get("license_number"),
        anniversary_date=_parse_date(data.get("anniversary_date")),
    )
    db.session.add(agent)
    db.session.commit()
    return _json({"agent": agent.to_dict(), "message": "Agent created"}, 201)


@compensation_bp.get("/agents")
def list_agents():
    include_inactive = request.args.get("include_inactive", "false").lower() == "true"
    q = Agent.query
    if not include_inactive:
        q = q.filter_by(is_active=True)
    agents = q.order_by(Agent.name_he.asc()).all()
    return _json({"agents": [a.to_dict() for a in agents], "count": len(agents)})


@compensation_bp.get("/agents/<int:agent_id>")
def get_agent(agent_id):
    agent = Agent.query.get_or_404(agent_id)
    fiscal_year = int(request.args.get("fiscal_year", date.today().year))
    ytd = _ytd_gci(agent_id, fiscal_year)
    tiers = _active_tiers()
    current_tier = _tier_for_gci(tiers, ytd)
    d = agent.to_dict(include_trainer=True)
    d["ytd_gci"] = round(ytd, 2)
    d["current_tier"] = current_tier.to_dict()
    return _json({"agent": d})


@compensation_bp.put("/agents/<int:agent_id>")
def update_agent(agent_id):
    agent = Agent.query.get_or_404(agent_id)
    data = request.get_json(silent=True) or {}
    for field in ("name_he", "name_en", "email", "phone", "license_number"):
        if field in data:
            setattr(agent, field, data[field])
    if "anniversary_date" in data:
        agent.anniversary_date = _parse_date(data["anniversary_date"])
    if "is_active" in data:
        agent.is_active = bool(data["is_active"])
    agent.updated_at = datetime.utcnow()
    db.session.commit()
    return _json({"agent": agent.to_dict(), "message": "Agent updated"})


@compensation_bp.delete("/agents/<int:agent_id>")
def deactivate_agent(agent_id):
    err = _require_admin()
    if err:
        return err
    agent = Agent.query.get_or_404(agent_id)
    agent.is_active = False
    agent.updated_at = datetime.utcnow()
    db.session.commit()
    return _json({"message": "Agent deactivated"})


@compensation_bp.put("/agents/<int:agent_id>/split")
def set_agent_split(agent_id):
    """Admin: set per-agent custom split override."""
    err = _require_admin()
    if err:
        return err
    agent = Agent.query.get_or_404(agent_id)
    data = request.get_json(silent=True) or {}

    threshold = data.get("override_threshold")
    agent_pct = data.get("override_agent_pct")

    if threshold is None and agent_pct is None:
        # Clear override
        agent.override_threshold = None
        agent.override_agent_pct = None
        msg = "ספליט אישי הוסר — חוזר למדרגות הגלובליות"
    else:
        if threshold is None or agent_pct is None:
            return _json({"error": "נדרשים גם override_threshold וגם override_agent_pct"}, 400)
        if not (0 < float(agent_pct) < 100):
            return _json({"error": "override_agent_pct חייב להיות בין 1 ל-99"}, 400)
        agent.override_threshold = float(threshold)
        agent.override_agent_pct = float(agent_pct)
        msg = f"ספליט אישי הוגדר: {agent_pct}% לסוכן מעל ₪{threshold:,.0f}"

    agent.updated_at = datetime.utcnow()
    db.session.commit()
    return _json({"agent": agent.to_dict(), "message": msg})


@compensation_bp.post("/agents/<int:agent_id>/trainer")
def assign_trainer(agent_id):
    agent = Agent.query.get_or_404(agent_id)
    data = request.get_json(silent=True) or {}
    trainer_id = data.get("trainer_id")
    if not trainer_id:
        return _json({"error": "trainer_id is required"}, 400)
    trainer = Agent.query.get_or_404(trainer_id)
    if trainer_id == agent_id:
        return _json({"error": "Agent cannot be their own trainer"}, 400)

    agent.trainer_id = trainer_id
    agent.trainer_since = _parse_date(data.get("trainer_since")) or date.today()
    agent.trainer_pct = float(data.get("trainer_pct", 10.0))
    agent.trainer_months = int(data.get("trainer_months", 6))
    agent.trainer_txn_cap = int(data.get("trainer_txn_cap", 0))
    agent.updated_at = datetime.utcnow()
    db.session.commit()
    return _json({
        "message": "Trainer assigned",
        "trainer": trainer.to_dict(),
        "trainee": agent.to_dict(),
    })


@compensation_bp.delete("/agents/<int:agent_id>/trainer")
def remove_trainer(agent_id):
    agent = Agent.query.get_or_404(agent_id)
    agent.trainer_id = None
    agent.trainer_since = None
    agent.trainer_pct = 10.0
    agent.trainer_months = 6
    agent.trainer_txn_cap = 0
    agent.updated_at = datetime.utcnow()
    db.session.commit()
    return _json({"message": "Trainer removed"})


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

@compensation_bp.post("/transactions")
def record_transaction():
    data = request.get_json(silent=True) or {}
    for f in ("agent_id", "gross_commission", "deal_date"):
        if f not in data:
            return _json({"error": f"Missing field: {f}"}, 400)

    agent_id = int(data["agent_id"])
    gross_commission = float(data["gross_commission"])
    deal_date = _parse_date(data["deal_date"])
    if not deal_date:
        return _json({"error": "Invalid deal_date format (use YYYY-MM-DD)"}, 400)
    if gross_commission <= 0:
        return _json({"error": "gross_commission must be positive"}, 400)

    result = calculate_commission(agent_id, gross_commission, deal_date)

    txn = Transaction(
        agent_id=agent_id,
        deal_date=deal_date,
        property_address=data.get("property_address"),
        property_city=data.get("property_city"),
        gross_commission=gross_commission,
        agent_split_pct=result.agent_split_pct,
        office_split_pct=result.office_split_pct,
        agent_amount=result.agent_amount,
        agent_cash_amount=result.agent_cash_amount,
        marketing_amount=result.marketing_amount,
        office_amount=result.office_amount_gross,
        trainer_override=result.trainer_override,
        tier_at_time=result.tier_at_time,
        ytd_gci_before=result.ytd_gci_before,
        ytd_gci_after=result.ytd_gci_after,
        fiscal_year=result.fiscal_year,
        notes=data.get("notes"),
    )
    db.session.add(txn)
    db.session.flush()  # get txn.id before committing

    # Log trainer override
    if result.trainer_override > 0 and result.trainer_id:
        override_log = TrainerOverride(
            transaction_id=txn.id,
            trainer_id=result.trainer_id,
            trainee_id=agent_id,
            override_amount=result.trainer_override,
            override_pct=Agent.query.get(agent_id).trainer_pct,
        )
        db.session.add(override_log)

    db.session.commit()

    return _json({
        "transaction": txn.to_dict(include_breakdown=True),
        "commission_breakdown": {
            "agent_amount": result.agent_amount,
            "office_amount_gross": result.office_amount_gross,
            "office_amount_net": result.office_amount_net,
            "trainer_override": result.trainer_override,
            "effective_agent_pct": result.agent_split_pct,
            "tier_at_time": result.tier_at_time,
            "ytd_gci_before": result.ytd_gci_before,
            "ytd_gci_after": result.ytd_gci_after,
            "tiers_crossed": [
                {
                    "tier": tc.tier_name,
                    "tier_he": tc.tier_name_he,
                    "portion": tc.portion,
                    "agent_pct": tc.agent_pct,
                    "agent_cut": tc.agent_cut,
                    "office_cut": tc.office_cut,
                }
                for tc in result.tiers_crossed
            ],
        },
        "message": "Transaction recorded",
    }, 201)


@compensation_bp.get("/transactions")
def list_transactions():
    agent_id = request.args.get("agent_id", type=int)
    fiscal_year = request.args.get("fiscal_year", type=int)
    limit = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))
    include_voided = request.args.get("include_voided", "false").lower() == "true"

    q = Transaction.query
    if agent_id:
        q = q.filter_by(agent_id=agent_id)
    if fiscal_year:
        q = q.filter_by(fiscal_year=fiscal_year)
    if not include_voided:
        q = q.filter_by(voided=False)

    total = q.count()
    txns = q.order_by(Transaction.deal_date.desc()).limit(limit).offset(offset).all()
    return _json({
        "transactions": [t.to_dict() for t in txns],
        "total": total,
        "limit": limit,
        "offset": offset,
    })


@compensation_bp.get("/transactions/<int:txn_id>")
def get_transaction(txn_id):
    txn = Transaction.query.get_or_404(txn_id)
    return _json({"transaction": txn.to_dict(include_breakdown=True)})


@compensation_bp.delete("/transactions/<int:txn_id>")
def void_transaction(txn_id):
    err = _require_admin()
    if err:
        return err
    txn = Transaction.query.get_or_404(txn_id)
    if txn.voided:
        return _json({"error": "Transaction already voided"}, 400)

    # Find subsequent transactions that may be affected
    subsequent = Transaction.query.filter(
        Transaction.agent_id == txn.agent_id,
        Transaction.fiscal_year == txn.fiscal_year,
        Transaction.deal_date >= txn.deal_date,
        Transaction.id != txn.id,
        Transaction.voided == False,  # noqa: E712
    ).order_by(Transaction.deal_date.asc()).all()

    txn.voided = True
    txn.updated_at = datetime.utcnow()
    db.session.commit()

    warning = None
    if subsequent:
        warning = (
            f"{len(subsequent)} subsequent transaction(s) may have incorrect "
            "ytd_gci_before/after. Use POST /comp/transactions/recalculate-year to fix."
        )

    return _json({
        "message": "Transaction voided",
        "warning": warning,
        "affected_transaction_ids": [t.id for t in subsequent],
    })


@compensation_bp.post("/transactions/recalculate-year")
def recalculate_year():
    err = _require_admin()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    agent_id = data.get("agent_id")
    fiscal_year = data.get("fiscal_year")
    if not agent_id or not fiscal_year:
        return _json({"error": "agent_id and fiscal_year are required"}, 400)

    agent = Agent.query.get_or_404(agent_id)
    txns = (Transaction.query
            .filter_by(agent_id=agent_id, fiscal_year=int(fiscal_year), voided=False)
            .order_by(Transaction.deal_date.asc(), Transaction.id.asc())
            .all())

    running_ytd = 0.0
    for txn in txns:
        result = calculate_commission(
            agent_id=agent_id,
            gross_commission=txn.gross_commission,
            deal_date=txn.deal_date,
        )
        # Re-snapshot using recalculated values
        txn.ytd_gci_before = result.ytd_gci_before
        txn.ytd_gci_after = result.ytd_gci_after
        txn.agent_split_pct = result.agent_split_pct
        txn.office_split_pct = result.office_split_pct
        txn.agent_amount = result.agent_amount
        txn.office_amount = result.office_amount_gross
        txn.trainer_override = result.trainer_override
        txn.tier_at_time = result.tier_at_time
        txn.updated_at = datetime.utcnow()
        running_ytd = result.ytd_gci_after

    db.session.commit()
    return _json({
        "message": f"Recalculated {len(txns)} transactions for agent {agent_id}, year {fiscal_year}",
        "final_ytd_gci": round(running_ytd, 2),
    })


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@compensation_bp.get("/dashboard")
def office_leaderboard():
    fiscal_year = int(request.args.get("fiscal_year", date.today().year))
    limit = min(int(request.args.get("limit", 20)), 100)
    tiers = _active_tiers()

    agents = Agent.query.filter_by(is_active=True).all()

    rows = []
    for agent in agents:
        ytd = _ytd_gci(agent.id, fiscal_year)
        current_tier = _tier_for_gci(tiers, ytd)

        # Next tier gap
        next_tier = None
        for t in sorted(tiers, key=lambda x: x.min_gci):
            if t.min_gci > ytd:
                next_tier = t
                break

        tier_range = (next_tier.min_gci - current_tier.min_gci) if next_tier else 1
        progress_pct = min(round((ytd - current_tier.min_gci) / tier_range * 100, 1), 100.0) if next_tier else 100.0

        txn_count = Transaction.query.filter_by(
            agent_id=agent.id, fiscal_year=fiscal_year, voided=False
        ).count()

        # Target vs actual
        target_ann = agent.target_annual or 0
        target_pct = round(ytd / target_ann * 100, 1) if target_ann > 0 else None

        rows.append({
            "agent_id": agent.id,
            "name_he": agent.name_he,
            "name_en": agent.name_en,
            "office_tab": agent.office_tab,
            "ytd_gci": round(ytd, 2),
            "current_tier": current_tier.tier_name,
            "current_tier_he": current_tier.tier_name_he,
            "badge_color": current_tier.badge_color,
            "progress_pct": progress_pct,
            "gap_to_next_tier": round(next_tier.min_gci - ytd, 2) if next_tier else 0,
            "transaction_count": txn_count,
            "target_annual": target_ann or None,
            "target_quarterly": agent.target_quarterly,
            "target_pct": target_pct,
        })

    rows.sort(key=lambda x: x["ytd_gci"], reverse=True)
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    rows = rows[:limit]

    # Office totals
    from sqlalchemy import func
    totals_row = db.session.execute(
        db.select(
            func.coalesce(func.sum(Transaction.gross_commission), 0.0),
            func.coalesce(func.sum(Transaction.agent_amount), 0.0),
            func.coalesce(func.sum(Transaction.office_amount), 0.0),
            func.coalesce(func.sum(Transaction.trainer_override), 0.0),
        )
        .where(Transaction.fiscal_year == fiscal_year)
        .where(Transaction.voided == False)  # noqa: E712
    ).one()

    total_gci, total_agent, total_office, total_overrides = [float(x) for x in totals_row]

    return _json({
        "fiscal_year": fiscal_year,
        "leaderboard": rows,
        "office_totals": {
            "total_gci": round(total_gci, 2),
            "total_agent_earnings": round(total_agent, 2),
            "total_office_earnings_gross": round(total_office, 2),
            "total_office_earnings_net": round(total_office - total_overrides, 2),
            "total_trainer_overrides": round(total_overrides, 2),
        },
    })


@compensation_bp.get("/dashboard/<int:agent_id>")
def agent_dashboard(agent_id):
    fiscal_year = request.args.get("fiscal_year", type=int)
    data = get_agent_dashboard(agent_id, fiscal_year)
    return _json(data)


@compensation_bp.get("/dashboard/trainer/<int:trainer_id>")
def trainer_dashboard(trainer_id):
    trainer = Agent.query.get_or_404(trainer_id)
    fiscal_year = int(request.args.get("fiscal_year", date.today().year))

    trainees = Agent.query.filter_by(trainer_id=trainer_id).all()
    trainees_data = []
    total_override_ytd = 0.0

    for trainee in trainees:
        ytd_override = db.session.execute(
            db.select(db.func.coalesce(db.func.sum(TrainerOverride.override_amount), 0.0))
            .join(Transaction, TrainerOverride.transaction_id == Transaction.id)
            .where(TrainerOverride.trainer_id == trainer_id)
            .where(TrainerOverride.trainee_id == trainee.id)
            .where(Transaction.fiscal_year == fiscal_year)
            .where(Transaction.voided == False)  # noqa: E712
        ).scalar()
        ytd_override = float(ytd_override or 0.0)
        total_override_ytd += ytd_override

        trainees_data.append({
            "agent": trainee.to_dict(),
            "override_pct": trainee.trainer_pct,
            "ytd_override_earnings": round(ytd_override, 2),
        })

    all_time = db.session.execute(
        db.select(db.func.coalesce(db.func.sum(TrainerOverride.override_amount), 0.0))
        .where(TrainerOverride.trainer_id == trainer_id)
    ).scalar()

    return _json({
        "trainer": trainer.to_dict(),
        "fiscal_year": fiscal_year,
        "trainees": trainees_data,
        "total_override_earnings_ytd": round(total_override_ytd, 2),
        "total_override_earnings_alltime": round(float(all_time or 0.0), 2),
    })


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

@compensation_bp.post("/simulate")
def simulate():
    data = request.get_json(silent=True) or {}
    additional_gci = float(data.get("additional_gci", 0))
    if additional_gci <= 0:
        return _json({"error": "additional_gci must be positive"}, 400)

    agent_id = data.get("agent_id")
    if agent_id:
        agent = Agent.query.get_or_404(int(agent_id))
        fiscal_year = int(data.get("fiscal_year", date.today().year))
        starting_ytd = _ytd_gci(int(agent_id), fiscal_year)
    else:
        starting_ytd = float(data.get("starting_ytd_gci", 0.0))

    result = simulate_earnings(starting_ytd, additional_gci)
    return _json({"simulation": result})


# ---------------------------------------------------------------------------
# Google Sheets Sync (called by Zapier)
# ---------------------------------------------------------------------------
#
# Zapier sends one POST per agent row whenever the sheet changes.
# Body (from Zapier "Webhooks by Zapier" action):
# {
#   "sync_key": "<COMP_ADMIN_KEY>",
#   "tab": "רחובות",
#   "name": "אפי",
#   "total_commission": 148425,
#   "transaction_count": 4,
#   "target_annual": 1000000,
#   "target_quarterly": 275000
# }
#
# Logic:
#  - Find or create agent by name + tab
#  - Update target fields
#  - Calculate delta vs current YTD GCI → record as transaction if positive

@compensation_bp.post("/sync/sheets")
def sync_from_sheets():
    data = request.get_json(silent=True) or {}

    # Auth: accept sync_key in body or X-Admin-Key header
    provided_key = data.get("sync_key") or request.headers.get("X-Admin-Key", "")
    if ADMIN_KEY and provided_key != ADMIN_KEY:
        return _json({"error": "Unauthorised"}, 401)

    name = (data.get("name") or "").strip()
    if not name:
        return _json({"error": "name is required"}, 400)

    try:
        total_commission = float(data.get("total_commission") or 0)
    except (ValueError, TypeError):
        return _json({"error": "total_commission must be a number"}, 400)

    tab          = (data.get("tab") or "").strip() or None
    target_ann   = float(data.get("target_annual") or 0) or None
    target_qrt   = float(data.get("target_quarterly") or 0) or None
    fiscal_year  = int(data.get("fiscal_year") or date.today().year)

    # Find or create agent by name + tab (tab is the city branch)
    q = Agent.query.filter(Agent.name_he == name, Agent.is_active == True)  # noqa: E712
    if tab:
        q = q.filter(Agent.office_tab == tab)
    agent = q.first()
    if not agent:
        agent = Agent(name_he=name, office_tab=tab)
        db.session.add(agent)
        db.session.flush()

    # Update targets + tab
    if target_ann is not None:
        agent.target_annual = target_ann
    if target_qrt is not None:
        agent.target_quarterly = target_qrt
    if tab:
        agent.office_tab = tab
    agent.updated_at = datetime.utcnow()

    # Calculate delta vs what we already have in DB
    current_ytd = _ytd_gci(agent.id, fiscal_year)
    delta = round(total_commission - current_ytd, 2)

    txn = None
    if delta > 0:
        deal_date = date.today()
        result = calculate_commission(agent.id, delta, deal_date)
        txn = Transaction(
            agent_id=agent.id,
            deal_date=deal_date,
            gross_commission=delta,
            agent_split_pct=result.agent_split_pct,
            office_split_pct=result.office_split_pct,
            agent_amount=result.agent_amount,
            agent_cash_amount=result.agent_cash_amount,
            marketing_amount=result.marketing_amount,
            office_amount=result.office_amount_gross,
            trainer_override=result.trainer_override,
            tier_at_time=result.tier_at_time,
            ytd_gci_before=result.ytd_gci_before,
            ytd_gci_after=result.ytd_gci_after,
            fiscal_year=result.fiscal_year,
            notes=f"סנכרון מגיליון Google Sheets — טאב: {tab or '—'}",
        )
        db.session.add(txn)
        if result.trainer_override > 0 and result.trainer_id:
            db.session.add(TrainerOverride(
                transaction_id=txn.id if txn.id else 0,
                trainer_id=result.trainer_id,
                trainee_id=agent.id,
                override_amount=result.trainer_override,
                override_pct=agent.trainer_pct,
            ))

    db.session.commit()

    return _json({
        "status": "ok",
        "agent": agent.to_dict(),
        "current_ytd": current_ytd,
        "sheet_total": total_commission,
        "delta_recorded": delta if delta > 0 else 0,
        "transaction_created": txn is not None,
    })
