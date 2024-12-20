
"""future considerations for cashflows_and_benchmark_tables.py"""

# class bond curves, all to do wiht the curves - so accumulate functions together and use same attributes - items

# so i know its the PV of CURVES
# maybe should inherit shocks but / or keep it simple for now (simplify first, that consider later)
# use inheritance for this (to hide it)
"""
class Curves:
    def __init__(self, GivenDate: pd.dt):
       self.GivenDate = GivenDate
    def get_pv(self):
"""
"""
class Curves:

    #>>> bond_curves = Curves()
    #>>> pv = bond_curves.get_pv()
    #>>> print(pv.type)
    #>>> print(pv.shape)
    #>>> print(pv)
        # Class attr: less common, don't necessarily do - more mem? curve_pv = None
    def __init__(self):
    # self.GivenDate = GivenDate
        self.curve_pv = None
    def get_pv(self):
        if self.curve_pv is None:
            pv = helpers.create_general_shock_table()
            self.curve_pv = pv[0]
            return self.curve_pv
        else:
            return self.curve_pv
"""
