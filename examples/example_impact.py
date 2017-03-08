
from __future__ import absolute_import
from impactlab import impactlab
from impactlab.mockfs import DataAPI, Variable, SuperIndex

import pandas as pd

def initialize_data():
    '''
    Set up the data in the API
    '''

    api = DataAPI()

    api.superindices = {
        'rcp': SuperIndex('rcp', 'Representative Concentration Pathways', [{'rcp': 'rcp{}'.format(r)} for r in [26, 45, 60, 85]]),
        'ssp': SuperIndex('ssp', 'Shared Socioeconomic Pathway', [{'ssp': 'ssp{}'.format(s)} for s in range(1,6)])
    }

    variables = [
        Variable('/GCP/socioeconomics/popop', api.superindices['ssp']),
        Variable('/GCP/climate/tas', api.superindices['rcp']),
        Variable('/GCP/impacts/mortality', api.superindices['rcp']*api.superindices['ssp']),
        Variable('/GCP/climate/tas2_ir', api.superindices['rcp'])]

    api.variables = {v.name: v for v in variables}

    return api

api = initialize_data()


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
