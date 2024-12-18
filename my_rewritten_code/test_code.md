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

"""
if copied.equals(KRDs):
    print("No changes were made to KRDs data")
else:
    print("The data has been modified")
"""