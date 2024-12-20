""" Changelog Brenda (09-30-24):
    I've now modified it to generate KRDs and hold them to pass down in memory rather than generate new KRDs during 
    each sector's back-to-back optimization.

    Considering, optimization and writing KRD solutions to excel file happen back-to-back hence most of stack memory
    holds it concurrently anyways.

    They are the same KRD profiles tables to write to excel in the very end, and used for every type of liability optimization. 
    Since, we have
        asset KRD profiles + list(liability segments)
    for optimization.
"""

""" Changelog Brenda (10-30-24):
    I've written a function create_summed_cashflows() to generate
    cashflows from solutions. I prefer using numpy, hence,
    matrix multiplication (vectorized) are used.

    Created more streamlined or speedier functions for shock tables
    and other data manipulation calculations.

    Decoupled calculations, reformatted and rewritten functions (except some legacy renaming and 
    optimization calculations - 'index out of bounds'), integrating legacy versions and
    new for compatibility and familiarity.
    
    Created new functions for ones that were slow, keeping some
    old functions or comments (old comments are in green, on optimization_worker).

    Added comments.

    Calculated and replaced several key financial and weighting calculations. Fixed weighting procedures;
    cashflow present value calculations;  asset balance calculations for model portfolio;
    (to ensure thorough weighting is performed); implemented proper PV and coupon calculations; etc.

    More changes.

    Test books made (in Benchmarking/Tests/Brenda/selected_date folder). These files
    check intermediary and final steps for the model portfolio and custom_benchmarks, cfs.xlsx files
    processes. In other words, these test workbooks replicate and demonstrate the model portfolio process required
    and implemented in code. These excel files will generate cashflows, KRD profiles, cashflows from solutions,
    and tests the model portfolio solution and cashflows from solutions to verify matching. And solution dollar sensitivity tests.
    
    As is done in the code, up to the optim process. 
    
    This process was made and verified for financial accuracy and implementations in code were written to replicate and reflect it. Workbooks were
    iteratively improved, with some formatting changes.
        Includes:
        cashflows generation from FTSE universe
        up and down shocks applied to bond curves
        asset sensitivity shocks for cashflows; asset KRDs calculated with 
        simple financial formula, so this is 70 bucket sensitivities
        the above is weighted into 6 buckets
        etc.
"""

""" Changelog Brenda (10-30-24):
    Additional changes. Consider the model portfolio second version
    as different from prior version.
"""

Unrelated:

# test code for python:
"""
if copied.equals(KRDs):
    print("No changes were made to KRDs data")
else:
    print("The data has been modified")
"""