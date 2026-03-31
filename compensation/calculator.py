"""
Commission calculation engine.

Core function: calculate_commission()
  - Applies the graduated (tiered) split algorithm
  - Handles tier boundary crossings within a single transaction
  - Calculates trainer override from the office share
  - Returns an immutable CommissionResult dataclass
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from typing import Optional
from dateutil.relativedelta import relativedelta

from .models import Agent, TierConfig, Transaction, db


@dataclass
class TierPortion:
    tier_name: str
    tier_name_he: str
    portion: float
    agent_pct: float
    office_pct: float
    agent_cut: float
    office_cut: float


@dataclass
class CommissionResult:
    agent_id: int
    gross_commission: float
    agent_amount: float
    office_amount_gross: float          # before trainer override
    office_amount_net: float            # after trainer override
    trainer_override: float
    trainer_id: Optional[int]
    agent_split_pct: float              # effective blended % for this deal
    office_split_pct: float             # effective blended % for this deal
    tier_at_time: str                   # tier name the agent was IN at deal start
    ytd_gci_before: float
    ytd_gci_after: float
    fiscal_year: int
    tiers_crossed: list[TierPortion] = field(default_factory=list)


def _fiscal_year_start(agent: Agent, deal_date: date) -> date:
    """Return the start of the agent's current fiscal year."""
    if agent.anniversary_date:
        ann = agent.anniversary_date
        # Find the most recent anniversary on or before deal_date
        candidate = ann.replace(year=deal_date.year)
        if candidate > deal_date:
            candidate = ann.replace(year=deal_date.year - 1)
        return candidate
    return date(deal_date.year, 1, 1)


def _get_fiscal_year(agent: Agent, deal_date: date) -> int:
    """Return an integer representing the fiscal year (calendar year of the start)."""
    return _fiscal_year_start(agent, deal_date).year


def _ytd_gci(agent_id: int, fiscal_year: int, exclude_transaction_id: int = None) -> float:
    """Sum of gross_commission for all non-voided transactions in the fiscal year."""
    q = Transaction.query.filter_by(agent_id=agent_id, fiscal_year=fiscal_year, voided=False)
    if exclude_transaction_id:
        q = q.filter(Transaction.id != exclude_transaction_id)
    result = db.session.execute(
        db.select(db.func.coalesce(db.func.sum(Transaction.gross_commission), 0.0))
        .where(Transaction.agent_id == agent_id)
        .where(Transaction.fiscal_year == fiscal_year)
        .where(Transaction.voided == False)  # noqa: E712
        .where(True if not exclude_transaction_id else Transaction.id != exclude_transaction_id)
    ).scalar()
    return float(result or 0.0)


def _active_tiers() -> list[TierConfig]:
    return TierConfig.query.filter_by(is_active=True).order_by(TierConfig.min_gci.asc()).all()


def _tier_for_gci(tiers: list[TierConfig], gci: float) -> TierConfig:
    """Return the tier that applies when YTD GCI equals `gci`."""
    matched = tiers[0]
    for t in tiers:
        if gci >= t.min_gci:
            matched = t
    return matched


def _is_trainer_override_active(agent: Agent, deal_date: date, trainee_txn_count: int) -> bool:
    """Determine whether the trainer override still applies for this deal."""
    if not agent.trainer_id:
        return False
    if agent.trainer_txn_cap and agent.trainer_txn_cap > 0:
        # Count-based: number of transactions that have already generated an override
        past_overrides = Transaction.query.filter(
            Transaction.agent_id == agent.id,
            Transaction.trainer_override > 0,
            Transaction.voided == False,  # noqa: E712
        ).count()
        return past_overrides < agent.trainer_txn_cap
    else:
        # Time-based
        if not agent.trainer_since:
            return False
        cutoff = agent.trainer_since + relativedelta(months=agent.trainer_months or 6)
        return deal_date <= cutoff


def calculate_commission(agent_id: int, gross_commission: float, deal_date: date) -> CommissionResult:
    """
    Calculate the agent/office split for a given commission amount.

    The graduated split algorithm handles tier crossings within a single
    transaction: if the commission pushes the agent from one tier to the next,
    each portion is calculated at the correct rate.
    """
    agent = Agent.query.get_or_404(agent_id)
    fiscal_year = _get_fiscal_year(agent, deal_date)
    ytd_before = _ytd_gci(agent_id, fiscal_year)
    tiers = _active_tiers()

    # --- Build effective tier list (inject per-agent override if set) ---
    # If the agent has a custom override, we synthesise a virtual tier above the threshold.
    effective_tiers = list(tiers)
    if agent.override_threshold is not None and agent.override_agent_pct is not None:
        # Find the global tier that contains the threshold and cap it there,
        # then append a virtual "override" tier above the threshold.
        capped = []
        for t in effective_tiers:
            if t.max_gci is None or t.max_gci > agent.override_threshold:
                # Cap this tier at the threshold
                from dataclasses import dataclass as _dc

                class _VirtualTier:
                    def __init__(self, tier_name, tier_name_he, min_gci, max_gci, agent_pct, office_pct):
                        self.tier_name = tier_name
                        self.tier_name_he = tier_name_he
                        self.min_gci = min_gci
                        self.max_gci = max_gci
                        self.agent_pct = agent_pct
                        self.office_pct = office_pct

                if t.min_gci < agent.override_threshold:
                    capped.append(_VirtualTier(
                        t.tier_name, t.tier_name_he,
                        t.min_gci, agent.override_threshold,
                        t.agent_pct, t.office_pct,
                    ))
                # Insert custom override tier (unlimited ceiling unless another tier exists above)
                override_office_pct = round(100.0 - agent.override_agent_pct, 4)
                capped.append(_VirtualTier(
                    "הסכם אישי", "הסכם אישי",
                    agent.override_threshold, None,
                    agent.override_agent_pct, override_office_pct,
                ))
                break
            else:
                capped.append(t)
        effective_tiers = capped

    # --- Graduated split ---
    remaining = gross_commission
    cursor = ytd_before
    agent_amount = 0.0
    office_amount = 0.0
    tiers_crossed: list[TierPortion] = []

    for tier in effective_tiers:
        if remaining <= 0:
            break
        # Skip tiers entirely below cursor
        if tier.max_gci is not None and cursor >= tier.max_gci:
            continue

        # How much room is left in this tier from cursor's position?
        if tier.max_gci is None:
            room = remaining
        else:
            room = tier.max_gci - cursor

        portion = min(remaining, room)
        if portion <= 0:
            continue

        agent_cut = round(portion * tier.agent_pct / 100, 2)
        office_cut = round(portion * tier.office_pct / 100, 2)

        agent_amount += agent_cut
        office_amount += office_cut
        tiers_crossed.append(TierPortion(
            tier_name=tier.tier_name,
            tier_name_he=tier.tier_name_he or "",
            portion=round(portion, 2),
            agent_pct=tier.agent_pct,
            office_pct=tier.office_pct,
            agent_cut=agent_cut,
            office_cut=office_cut,
        ))

        cursor += portion
        remaining -= portion

    agent_amount = round(agent_amount, 2)
    office_amount = round(office_amount, 2)

    # Blended effective percentages (for snapshot storage)
    agent_split_pct = round(agent_amount / gross_commission * 100, 4) if gross_commission else 0.0
    office_split_pct = round(100.0 - agent_split_pct, 4)

    # Tier the agent was IN at the start of this deal
    tier_at_time = _tier_for_gci(tiers, ytd_before).tier_name

    # --- Trainer override ---
    trainee_txn_count = Transaction.query.filter_by(
        agent_id=agent_id, voided=False
    ).count()
    trainer_override = 0.0
    trainer_id = agent.trainer_id

    if _is_trainer_override_active(agent, deal_date, trainee_txn_count):
        trainer_override = round(office_amount * (agent.trainer_pct / 100), 2)

    ytd_after = round(ytd_before + gross_commission, 2)

    return CommissionResult(
        agent_id=agent_id,
        gross_commission=gross_commission,
        agent_amount=agent_amount,
        office_amount_gross=office_amount,
        office_amount_net=round(office_amount - trainer_override, 2),
        trainer_override=trainer_override,
        trainer_id=trainer_id,
        agent_split_pct=agent_split_pct,
        office_split_pct=office_split_pct,
        tier_at_time=tier_at_time,
        ytd_gci_before=ytd_before,
        ytd_gci_after=ytd_after,
        fiscal_year=fiscal_year,
        tiers_crossed=tiers_crossed,
    )


def simulate_earnings(starting_ytd_gci: float, additional_gci: float) -> dict:
    """
    Hypothetical earnings simulation — no agent ID needed.
    Returns a breakdown of how `additional_gci` would be split,
    starting from `starting_ytd_gci`.
    """
    tiers = _active_tiers()

    remaining = additional_gci
    cursor = starting_ytd_gci
    agent_amount = 0.0
    office_amount = 0.0
    tiers_crossed = []

    for tier in tiers:
        if remaining <= 0:
            break
        if tier.max_gci is not None and cursor >= tier.max_gci:
            continue

        room = additional_gci if tier.max_gci is None else (tier.max_gci - cursor)
        portion = min(remaining, room)
        if portion <= 0:
            continue

        agent_cut = round(portion * tier.agent_pct / 100, 2)
        office_cut = round(portion * tier.office_pct / 100, 2)
        agent_amount += agent_cut
        office_amount += office_cut
        tiers_crossed.append({
            "tier": tier.tier_name,
            "tier_he": tier.tier_name_he,
            "portion": round(portion, 2),
            "agent_pct": tier.agent_pct,
            "agent_cut": agent_cut,
            "office_cut": office_cut,
        })
        cursor += portion
        remaining -= portion

    ending_ytd = starting_ytd_gci + additional_gci
    effective_agent_pct = round(agent_amount / additional_gci * 100, 2) if additional_gci else 0

    # Next tier after the simulation ends
    ending_tier = _tier_for_gci(tiers, ending_ytd)
    next_tier = None
    for t in sorted(tiers, key=lambda x: x.min_gci):
        if t.min_gci > ending_ytd:
            next_tier = {
                "name": t.tier_name,
                "name_he": t.tier_name_he,
                "min_gci": t.min_gci,
                "agent_pct": t.agent_pct,
                "gap_remaining": round(t.min_gci - ending_ytd, 2),
            }
            break

    return {
        "starting_ytd_gci": starting_ytd_gci,
        "ending_ytd_gci": round(ending_ytd, 2),
        "gross_commission_simulated": additional_gci,
        "agent_earnings": round(agent_amount, 2),
        "office_earnings": round(office_amount, 2),
        "effective_agent_pct": effective_agent_pct,
        "tiers_crossed": tiers_crossed,
        "tier_at_start": _tier_for_gci(tiers, starting_ytd_gci).tier_name,
        "tier_at_end": ending_tier.tier_name,
        "next_tier_after_this": next_tier,
    }


def get_agent_dashboard(agent_id: int, fiscal_year: int = None) -> dict:
    """Build the full dashboard payload for a single agent."""
    from datetime import date as date_cls
    agent = Agent.query.get_or_404(agent_id)
    tiers = _active_tiers()

    if fiscal_year is None:
        fiscal_year = _get_fiscal_year(agent, date_cls.today())

    # YTD aggregates
    rows = db.session.execute(
        db.select(
            db.func.coalesce(db.func.sum(Transaction.gross_commission), 0.0),
            db.func.coalesce(db.func.sum(Transaction.agent_amount), 0.0),
            db.func.coalesce(db.func.sum(Transaction.office_amount), 0.0),
            db.func.coalesce(db.func.sum(Transaction.trainer_override), 0.0),
            db.func.count(Transaction.id),
        )
        .where(Transaction.agent_id == agent_id)
        .where(Transaction.fiscal_year == fiscal_year)
        .where(Transaction.voided == False)  # noqa: E712
    ).one()

    ytd_gci, ytd_agent_earn, ytd_office_gross, ytd_overrides_paid, txn_count = rows
    ytd_gci = float(ytd_gci)
    ytd_agent_earn = float(ytd_agent_earn)
    ytd_office_gross = float(ytd_office_gross)
    ytd_overrides_paid = float(ytd_overrides_paid)
    txn_count = int(txn_count)

    current_tier = _tier_for_gci(tiers, ytd_gci)
    next_tier_obj = None
    for t in sorted(tiers, key=lambda x: x.min_gci):
        if t.min_gci > ytd_gci:
            next_tier_obj = t
            break

    next_tier_info = None
    if next_tier_obj:
        tier_range = next_tier_obj.min_gci - current_tier.min_gci
        progress_in_range = ytd_gci - current_tier.min_gci
        progress_pct = round(progress_in_range / tier_range * 100, 1) if tier_range > 0 else 100.0
        next_tier_info = {
            "name": next_tier_obj.tier_name,
            "name_he": next_tier_obj.tier_name_he,
            "min_gci": next_tier_obj.min_gci,
            "agent_pct": next_tier_obj.agent_pct,
            "gap": round(next_tier_obj.min_gci - ytd_gci, 2),
            "progress_pct": min(progress_pct, 99.9),
        }
    else:
        # Already at highest tier
        next_tier_info = None

    # Recent transactions
    recent_txns = (Transaction.query
                   .filter_by(agent_id=agent_id, fiscal_year=fiscal_year, voided=False)
                   .order_by(Transaction.deal_date.desc())
                   .limit(5)
                   .all())

    # Trainer info
    trainer_info = None
    trainer_override_active = False
    trainer_override_expires = None
    trainer_overrides_remaining = None

    if agent.trainer_id:
        trainer_info = agent.trainer.to_dict() if agent.trainer else None
        trainer_override_active = _is_trainer_override_active(agent, date_cls.today(), txn_count)

        if agent.trainer_txn_cap and agent.trainer_txn_cap > 0:
            past = Transaction.query.filter(
                Transaction.agent_id == agent_id,
                Transaction.trainer_override > 0,
                Transaction.voided == False,  # noqa: E712
            ).count()
            trainer_overrides_remaining = max(0, agent.trainer_txn_cap - past)
        elif agent.trainer_since:
            cutoff = agent.trainer_since + relativedelta(months=agent.trainer_months or 6)
            trainer_override_expires = cutoff.isoformat()

    return {
        "agent": agent.to_dict(include_trainer=False),
        "fiscal_year": fiscal_year,
        "ytd_gci": round(ytd_gci, 2),
        "current_tier": current_tier.to_dict(),
        "next_tier": next_tier_info,
        "ytd_agent_earnings": round(ytd_agent_earn, 2),
        "ytd_office_earnings_gross": round(ytd_office_gross, 2),
        "ytd_office_earnings_net": round(ytd_office_gross - ytd_overrides_paid, 2),
        "ytd_trainer_overrides_paid": round(ytd_overrides_paid, 2),
        "transaction_count": txn_count,
        "trainer": trainer_info,
        "trainer_override_active": trainer_override_active,
        "trainer_override_expires": trainer_override_expires,
        "trainer_overrides_remaining": trainer_overrides_remaining,
        "recent_transactions": [t.to_dict() for t in recent_txns],
    }
