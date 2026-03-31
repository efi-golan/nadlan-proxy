"""
SQLAlchemy models for the agent compensation system.
"""
from datetime import datetime, date
from . import db


class Agent(db.Model):
    __tablename__ = "agents"

    id               = db.Column(db.Integer, primary_key=True)
    name_he          = db.Column(db.Text, nullable=False)          # Hebrew full name
    name_en          = db.Column(db.Text)
    email            = db.Column(db.Text, unique=True)
    phone            = db.Column(db.Text)
    license_number   = db.Column(db.Text, unique=True)

    # Trainer relationship (self-referential)
    trainer_id       = db.Column(db.Integer, db.ForeignKey("agents.id"), nullable=True)
    trainer_since    = db.Column(db.Date, nullable=True)
    trainer_txn_cap  = db.Column(db.Integer, default=0)   # 0 = time-based mode
    trainer_months   = db.Column(db.Integer, default=6)   # months of override
    trainer_pct      = db.Column(db.Float, default=10.0)  # % from office share

    # Fiscal year reset: if NULL, use Jan 1 each year
    anniversary_date = db.Column(db.Date, nullable=True)

    # Targets (pulled from Google Sheets)
    target_annual    = db.Column(db.Float, nullable=True)    # יעד שנתי
    target_quarterly = db.Column(db.Float, nullable=True)    # יעד רבעוני
    office_tab       = db.Column(db.Text, nullable=True)     # רחובות / יבנה

    # Per-agent custom split override (admin-configurable)
    # If set, above override_threshold the agent earns override_agent_pct instead of tier default
    override_threshold = db.Column(db.Float, nullable=True)  # GCI סף (e.g. 250000)
    override_agent_pct = db.Column(db.Float, nullable=True)  # % לסוכן מעל הסף (e.g. 60.0)

    is_active        = db.Column(db.Boolean, default=True)
    created_at       = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at       = db.Column(db.DateTime, default=datetime.utcnow,
                                 onupdate=datetime.utcnow)

    trainer    = db.relationship("Agent", remote_side=[id], backref="trainees",
                                 foreign_keys=[trainer_id])
    transactions = db.relationship("Transaction", backref="agent",
                                   foreign_keys="Transaction.agent_id",
                                   lazy="dynamic")

    def to_dict(self, include_trainer=False):
        d = {
            "id": self.id,
            "name_he": self.name_he,
            "name_en": self.name_en,
            "email": self.email,
            "phone": self.phone,
            "license_number": self.license_number,
            "trainer_id": self.trainer_id,
            "trainer_since": self.trainer_since.isoformat() if self.trainer_since else None,
            "trainer_txn_cap": self.trainer_txn_cap,
            "trainer_months": self.trainer_months,
            "trainer_pct": self.trainer_pct,
            "anniversary_date": self.anniversary_date.isoformat() if self.anniversary_date else None,
            "target_annual": self.target_annual,
            "target_quarterly": self.target_quarterly,
            "office_tab": self.office_tab,
            "override_threshold": self.override_threshold,
            "override_agent_pct": self.override_agent_pct,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
        if include_trainer and self.trainer:
            d["trainer"] = self.trainer.to_dict()
        return d


class Transaction(db.Model):
    __tablename__ = "transactions"

    id                = db.Column(db.Integer, primary_key=True)
    agent_id          = db.Column(db.Integer, db.ForeignKey("agents.id"), nullable=False)
    deal_date         = db.Column(db.Date, nullable=False)
    property_address  = db.Column(db.Text)
    property_city     = db.Column(db.Text)
    gross_commission  = db.Column(db.Float, nullable=False)

    # Snapshotted at recording time — immutable audit trail
    agent_split_pct   = db.Column(db.Float, nullable=False)
    office_split_pct  = db.Column(db.Float, nullable=False)
    agent_amount      = db.Column(db.Float, nullable=False)   # total agent share (cash + marketing)
    agent_cash_amount = db.Column(db.Float, nullable=True)    # cash to agent (always 50% base)
    marketing_amount  = db.Column(db.Float, nullable=True)    # portion invested in agent marketing
    office_amount     = db.Column(db.Float, nullable=False)   # before trainer override
    trainer_override  = db.Column(db.Float, nullable=False, default=0.0)
    tier_at_time      = db.Column(db.Text, nullable=False)
    ytd_gci_before    = db.Column(db.Float, nullable=False)
    ytd_gci_after     = db.Column(db.Float, nullable=False)
    fiscal_year       = db.Column(db.Integer, nullable=False)

    notes             = db.Column(db.Text)
    voided            = db.Column(db.Boolean, default=False)
    created_at        = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at        = db.Column(db.DateTime, default=datetime.utcnow,
                                  onupdate=datetime.utcnow)

    trainer_override_log = db.relationship("TrainerOverride", backref="transaction",
                                           cascade="all, delete-orphan")

    def to_dict(self, include_breakdown=False):
        d = {
            "id": self.id,
            "agent_id": self.agent_id,
            "deal_date": self.deal_date.isoformat() if self.deal_date else None,
            "property_address": self.property_address,
            "property_city": self.property_city,
            "gross_commission": self.gross_commission,
            "agent_split_pct": self.agent_split_pct,
            "office_split_pct": self.office_split_pct,
            "agent_amount": self.agent_amount,
            "agent_cash_amount": self.agent_cash_amount if self.agent_cash_amount is not None else self.agent_amount,
            "marketing_amount": self.marketing_amount or 0.0,
            "office_amount": self.office_amount,
            "office_amount_net": round(self.office_amount - self.trainer_override, 2),
            "trainer_override": self.trainer_override,
            "tier_at_time": self.tier_at_time,
            "ytd_gci_before": self.ytd_gci_before,
            "ytd_gci_after": self.ytd_gci_after,
            "fiscal_year": self.fiscal_year,
            "notes": self.notes,
            "voided": self.voided,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
        if include_breakdown:
            d["trainer_overrides"] = [o.to_dict() for o in self.trainer_override_log]
        return d


class TierConfig(db.Model):
    __tablename__ = "tier_config"

    id           = db.Column(db.Integer, primary_key=True)
    tier_name    = db.Column(db.Text, nullable=False)
    tier_name_he = db.Column(db.Text)
    min_gci      = db.Column(db.Float, nullable=False)
    max_gci      = db.Column(db.Float, nullable=True)   # NULL = unlimited
    agent_pct    = db.Column(db.Float, nullable=False)
    office_pct   = db.Column(db.Float, nullable=False)
    badge_color  = db.Column(db.Text)
    badge_icon   = db.Column(db.Text)
    sort_order   = db.Column(db.Integer, nullable=False, default=0)
    is_active    = db.Column(db.Boolean, default=True)
    effective_from = db.Column(db.Date, default=date(2024, 1, 1))

    def to_dict(self):
        return {
            "id": self.id,
            "tier_name": self.tier_name,
            "tier_name_he": self.tier_name_he,
            "min_gci": self.min_gci,
            "max_gci": self.max_gci,
            "agent_pct": self.agent_pct,
            "office_pct": self.office_pct,
            "badge_color": self.badge_color,
            "badge_icon": self.badge_icon,
            "sort_order": self.sort_order,
            "is_active": self.is_active,
            "effective_from": self.effective_from.isoformat() if self.effective_from else None,
        }


class TrainerOverride(db.Model):
    __tablename__ = "trainer_overrides"

    id              = db.Column(db.Integer, primary_key=True)
    transaction_id  = db.Column(db.Integer, db.ForeignKey("transactions.id"), nullable=False)
    trainer_id      = db.Column(db.Integer, db.ForeignKey("agents.id"), nullable=False)
    trainee_id      = db.Column(db.Integer, db.ForeignKey("agents.id"), nullable=False)
    override_amount = db.Column(db.Float, nullable=False)
    override_pct    = db.Column(db.Float, nullable=False)
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)

    trainer = db.relationship("Agent", foreign_keys=[trainer_id])
    trainee = db.relationship("Agent", foreign_keys=[trainee_id])

    def to_dict(self):
        return {
            "id": self.id,
            "transaction_id": self.transaction_id,
            "trainer_id": self.trainer_id,
            "trainee_id": self.trainee_id,
            "override_amount": self.override_amount,
            "override_pct": self.override_pct,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
