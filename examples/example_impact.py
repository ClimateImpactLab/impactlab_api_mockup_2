
from __future__ import absolute_import
from impactlab import impactlab
from impactlab.mockfs import DataAPI, Variable, SuperIndex

import pandas as pd

api = DataAPI()


@impactlab.uses(tas=api.get_variable('/GCP/climate/tas'))
@impactlab.iters()
def load_climate(tas):

    with tas.open('r') as f:
        tas_data = pd.read_csv(f)

    @impactlab.uses(tas=tas)
    @impactlab.uses(popop=api.get_variable('/GCP/socioeconomics/popop'))
    @impactlab.updates(api.get_variable('/GCP/climate/tas2_ir'))
    def tas2_ir(popop, tas):

        with popop.open('r') as f:
            popop_data = pd.read_csv(f)

        return (tas_data**2) * popop_data

    tas2_ir()


def compute_mortality(popop, tas):
    return popop.value * tas.value


@impactlab.uses(popop=api.get_variable('/GCP/socioeconomics/popop'), tas=api.get_variable('/GCP/climate/tas'))
@impactlab.updates(api.get_variable('/GCP/impacts/mortality'))
def mortality(popop, tas):
    return compute_mortality(popop, tas)


def main():
    load_climate()


if __name__ == '__main__':
    main()
