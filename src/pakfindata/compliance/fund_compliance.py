"""
Extensible compliance screening hooks for fund management entities.

These are PLACEHOLDER methods — actual screening algorithms will be
plugged in from WatchGuard PK (AML/CFT compliance platform).

Usage:
    from pakfindata.compliance.fund_compliance import AMCComplianceHook

    hook = AMCComplianceHook()  # No screener → returns NOT_SCREENED
    result = hook.screen_amc("Al Meezan Investment Management")
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional


class ComplianceScreener(ABC):
    """Base class for compliance screening."""

    @abstractmethod
    def screen_entity(self, entity_name: str, entity_type: str) -> dict:
        """Screen a fund management entity against watchlists.

        Args:
            entity_name: Name of entity to screen.
            entity_type: Type: "AMC", "DIRECTOR", "INVESTOR".

        Returns:
            Screening result dict.
        """

    @abstractmethod
    def get_risk_score(self, entity_name: str) -> float:
        """Return 0.0-1.0 risk score for entity."""


class AMCComplianceHook:
    """Screening hook for Asset Management Companies.

    Pluggable interface for WatchGuard PK integration.
    """

    def __init__(self, screener: Optional[ComplianceScreener] = None):
        self.screener = screener

    def screen_amc(self, amc_name: str) -> dict:
        """Screen AMC against sanctions and watchlists.

        Checks:
        - SECP sanctions list
        - SBP debarment list
        - FATF high-risk jurisdictions (for offshore subs)
        - UN/OFAC sanctions (for international AMCs)

        Args:
            amc_name: AMC name to screen.

        Returns:
            Dict with entity, screened_at, status, matches, risk_score, etc.
        """
        if self.screener is None:
            return {
                "entity": amc_name,
                "screened_at": date.today().isoformat(),
                "status": "NOT_SCREENED",
                "matches": [],
                "risk_score": 0.0,
                "lists_checked": [],
                "requires_review": False,
                "_note": "No compliance screener configured. Plug in WatchGuard PK.",
            }
        return self.screener.screen_entity(amc_name, "AMC")

    def screen_fund_directors(
        self, fund_name: str, directors: list[str]
    ) -> list[dict]:
        """Screen all directors/trustees of a fund.

        Args:
            fund_name: Fund name for context.
            directors: List of director names.

        Returns:
            List of screening results.
        """
        if self.screener is None:
            return [
                {
                    "entity": d,
                    "fund": fund_name,
                    "status": "NOT_SCREENED",
                    "screened_at": date.today().isoformat(),
                }
                for d in directors
            ]
        return [self.screener.screen_entity(d, "DIRECTOR") for d in directors]

    def check_beneficial_ownership(self, amc_name: str) -> dict:
        """Check UBO registry for AMC (SECP requirement).

        Args:
            amc_name: AMC name.

        Returns:
            UBO check result (placeholder).
        """
        return {
            "entity": amc_name,
            "status": "NOT_IMPLEMENTED",
            "_note": "UBO registry check — awaiting SECP API integration.",
        }

    def aml_transaction_monitoring(
        self, fund_name: str, flow_data: dict
    ) -> dict:
        """Monitor for suspicious fund flows.

        Detects:
        - Unusual large redemptions
        - Rapid subscription/redemption cycles (round-tripping)
        - Structuring (multiple small subs below reporting threshold)
        - PEP investments

        Args:
            fund_name: Fund name.
            flow_data: Dict with subscription/redemption flow data.

        Returns:
            Monitoring result (placeholder).
        """
        return {
            "fund": fund_name,
            "status": "NOT_IMPLEMENTED",
            "_note": "Transaction monitoring — awaiting WatchGuard PK engine.",
        }

    def ctf_screening(self, investor_name: str) -> dict:
        """Counter-Terrorism Financing screening for fund investors.

        Args:
            investor_name: Investor name.

        Returns:
            CTF screening result (placeholder).
        """
        if self.screener is None:
            return {
                "entity": investor_name,
                "status": "NOT_SCREENED",
                "screened_at": date.today().isoformat(),
            }
        return self.screener.screen_entity(investor_name, "INVESTOR")

    def cpf_check(self, fund_name: str) -> dict:
        """Counter Proliferation Financing check.

        Screens fund holdings against dual-use goods entities.

        Args:
            fund_name: Fund name.

        Returns:
            CPF check result (placeholder).
        """
        return {
            "fund": fund_name,
            "status": "NOT_IMPLEMENTED",
            "_note": "CPF holdings check — requires portfolio data integration.",
        }
