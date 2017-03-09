
from __future__ import absolute_import
from impactlab import impactlab
from impactlab.mockfs import DataAPI

import pandas as pd

api = DataAPI()


def compute_mortality(popop, tas):
    '''
    Demonstrates a simple atomic computation
    '''

    return popop.value * tas.value


@impactlab.uses(popop=api.get_variable('/GCP/socioeconomics/popop'), tas=api.get_variable('/GCP/climate/tas'))
@impactlab.updates(api.get_variable('/GCP/impacts/mortality'))
def mortality(popop, tas):
    '''
    Demonstrates a simple computation job

    The impactlab.uses decorator accepts keyword arguments of the form 
    {name: obj}, where name is the name of argument to pass and obj is a mockfs
    Variable object.

    The impactlab.updates decorator drives a for-loop over all combinations of
    indices for the given variables. The value returned by the decorated
    function is used to update the value of the variable for the given indices.
    '''

    return compute_mortality(popop, tas)



@impactlab.uses(tas=api.get_variable('/GCP/climate/tas'))
@impactlab.iters()
def tas2_ir(tas):
    '''
    Demonstrates a two-stage ETL process
    '''

    with tas.open('r') as f:
        tas_data = pd.read_csv(f)

    @impactlab.uses(tas=tas)
    @impactlab.uses(popop=api.get_variable('/GCP/socioeconomics/popop'))
    @impactlab.updates(api.get_variable('/GCP/climate/tas2_ir'))
    def inner(popop, tas):
        '''
        The inner loop's uses() decorator is given tas as an argument. The
        update decorator sees that tas is an archive rather than a variable and
        simply passes the value through rather than attempting to loop over it.
        '''

        with popop.open('r') as f:
            popop_data = pd.read_csv(f)

        return (tas_data**2) * popop_data

    inner()



def main():
    print('\nStarting job "tas2_ir"\n{}'.format('='*50))
    tas2_ir()

    print('\nStarting job "mortality"\n{}'.format('='*50))
    mortality()



if __name__ == '__main__':
    main()
