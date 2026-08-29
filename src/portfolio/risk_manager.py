"""
💎 V25.0 APEX QUANTUM PRIME: COMPATIBILITY SHIM
------------------------------------------------------------
Maintains backward compatibility for legacy imports during the 
V25.0 architectural migration. Re-exports InstitutionalRiskVault 
from the newly optimized risk_vault module.
"""

from portfolio.risk_vault import InstitutionalRiskVault

__all__ = ["InstitutionalRiskVault"]