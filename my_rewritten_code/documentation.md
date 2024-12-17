"""
Definitions

ASSET CLASS: mortgage, public, private (split the items into ratings or UL, PAR, SURPLUS, etc)

--- separate ---

SEGMENTS: UL, PAR, SURPLUS
- a.k.a. 'portfolio'
These are broader categories for assets, each of which has a different allocated asset mix.

RATINGS:
- a.k.a. asset types, asset mix

To determine the asset types per segment, multiply the segment balance by the asset mix percentage that is unique to each
segment. This percentage matrix is called the "Asset Mix.xlsx" file, where the segment balances and total asset balance is 
called the "SBS Totals.xlsx" file. # This holds the liabilities, asset, and totals balance for all segments.

A: Assets
1. Calculate cashflows from FTSE universe.
2. Calculate KRDs from cashflows from FTSE universe.

B:
1. Bring in liability sensitivities through balance sheet and asset percentage matrix. The liabilities for our holdings. (FALSE)
1. Bring in liability sensitivities through "Targets by Asset Class.xlsx" as the targets to hedge asset krds and liabilities to.

OPTIMIZATION:
The calculated KRDs (simple) and brought-in KRDs from "Targets by Asset Class.xlsx" (to match) are matched during this 
process.

We essentially match the asset KRDs to liability KRDs for liabilities hedging, and perform an optimization function to 
maximize returns for Totals.
"""