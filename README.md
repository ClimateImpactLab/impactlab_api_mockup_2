# Mockup ImpactLab API, part 2

This mockup is a proof of concept for using decorators to control execution for typical Climate Impact Lab pipeline jobs. While none of this is live, this code can be executed using the mockup version of the DataFS backend we've implemented in this repo.

This readme is available as an ipython notebook [here](https://github.com/ClimateImpactLab/impactlab_api_mockup_2/tree/master/README.ipynb)

## Setup

```python
from __future__ import absolute_import
from impactlab import impactlab
from impactlab.mockfs import DataAPI, Variable, SuperIndex

import pandas as pd
```


```python
# initialize the mockup of a DataFS api with variable support
api = DataAPI()
```

## Specifying a simple pipeline job

The goal is to have the ability to write atomic pipeline components and then map them across our data sets.

#### Example: multiply `popop` by `tas`


```python
def compute_mortality(popop, tas):
    '''
    Demonstrates a simple atomic computation
    '''

    return popop.value * tas.value


```

This can be parameterized using the `impactlab.uses` functions and run using `impactlab.updates`:


```python
@impactlab.uses(popop=api.get_variable('/GCP/socioeconomics/popop'), tas=api.get_variable('/GCP/climate/tas'))
@impactlab.iters()
@impactlab.updates(api.get_variable('/GCP/impacts/mortality'))
def mortality(popop, tas):
    '''
    Demonstrates a simple computation job

    The `impactlab.uses` decorator accepts keyword arguments of the form 
    {name: obj}, where name is the name of argument to pass and obj is a mockfs
    Variable object.

    The `impactlab.iters` decorator drives a for-loop over all combinations of
    indices for the given variables.

    The value returned by the decorated function is used to update the value of
    the variable specified by `impactlab.updates` for the given indices.
    '''

    return compute_mortality(popop, tas)
```

To see this in action, simply call `mortality`:


```python
mortality()
```

    <Archive /GCP/impacts/mortality/rcp26/ssp1.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp45/ssp1.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp60/ssp1.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp85/ssp1.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp26/ssp2.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp45/ssp2.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp60/ssp2.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp85/ssp2.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp26/ssp3.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp45/ssp3.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp60/ssp3.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp85/ssp3.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp26/ssp4.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp45/ssp4.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp60/ssp4.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp85/ssp4.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp26/ssp5.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp45/ssp5.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp60/ssp5.nc> bumped 0.0.1 --> 0.0.2
    <Archive /GCP/impacts/mortality/rcp85/ssp5.nc> bumped 0.0.1 --> 0.0.2


To run a subset of the jobs, slice the variables in the `@impactlab.uses` calls:


```python
@impactlab.uses(popop=api.get_variable('/GCP/socioeconomics/popop'))
@impactlab.uses(tas=api.get_variable('/GCP/climate/tas')[{'rcp': 'rcp85'}])
@impactlab.iters()
@impactlab.updates(api.get_variable('/GCP/impacts/mortality'))
def mortality_rcp85(popop, tas):
    '''
    `impactlab_uses` may be supplied as many times as desired (but must be above
    impactlab.updates or other functional decorators).
    
    Slicing variables is done with a dictionary specifying the index to be sliced
    '''

    return compute_mortality(popop, tas)
```

Note that this function only iterates over `rcp85`:


```python
mortality_rcp85()
```

    <Archive /GCP/impacts/mortality/rcp85/ssp1.nc> bumped 0.0.2 --> 0.0.3
    <Archive /GCP/impacts/mortality/rcp85/ssp2.nc> bumped 0.0.2 --> 0.0.3
    <Archive /GCP/impacts/mortality/rcp85/ssp3.nc> bumped 0.0.2 --> 0.0.3
    <Archive /GCP/impacts/mortality/rcp85/ssp4.nc> bumped 0.0.2 --> 0.0.3
    <Archive /GCP/impacts/mortality/rcp85/ssp5.nc> bumped 0.0.2 --> 0.0.3


## Running a complex ETL Job

Let's say you want to perform a job that loads one class of variables into memory, then iterates over another set while keeping the first set alive.

This can be done using a two-stage job using the `iters` decorator:


```python
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
    @impactlab.iters()
    @impactlab.updates(api.get_variable('/GCP/climate/tas2_ir'))
    def inner(popop, tas):
        '''
        The inner loop's uses() decorator is given tas as an argument. The
        `iters` decorator sees that tas is an archive rather than a variable
        and simply passes the value through rather than attempting to loop over
        it.
        '''

        with popop.open('r') as f:
            popop_data = pd.read_csv(f)

        return (tas_data**2) * popop_data

    inner()
```

When this is run, note how the climate data is only loaded once per outer loop:


```python
tas2_ir()
```

    loading <Archive /GCP/climate/tas/rcp26.nc>
    loading <Archive /GCP/socioeconomics/popop/ssp1.nc>
    <Archive /GCP/climate/tas2_ir/rcp26/ssp1.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp2.nc>
    <Archive /GCP/climate/tas2_ir/rcp26/ssp2.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp3.nc>
    <Archive /GCP/climate/tas2_ir/rcp26/ssp3.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp4.nc>
    <Archive /GCP/climate/tas2_ir/rcp26/ssp4.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp5.nc>
    <Archive /GCP/climate/tas2_ir/rcp26/ssp5.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/climate/tas/rcp45.nc>
    loading <Archive /GCP/socioeconomics/popop/ssp1.nc>
    <Archive /GCP/climate/tas2_ir/rcp45/ssp1.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp2.nc>
    <Archive /GCP/climate/tas2_ir/rcp45/ssp2.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp3.nc>
    <Archive /GCP/climate/tas2_ir/rcp45/ssp3.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp4.nc>
    <Archive /GCP/climate/tas2_ir/rcp45/ssp4.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp5.nc>
    <Archive /GCP/climate/tas2_ir/rcp45/ssp5.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/climate/tas/rcp60.nc>
    loading <Archive /GCP/socioeconomics/popop/ssp1.nc>
    <Archive /GCP/climate/tas2_ir/rcp60/ssp1.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp2.nc>
    <Archive /GCP/climate/tas2_ir/rcp60/ssp2.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp3.nc>
    <Archive /GCP/climate/tas2_ir/rcp60/ssp3.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp4.nc>
    <Archive /GCP/climate/tas2_ir/rcp60/ssp4.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp5.nc>
    <Archive /GCP/climate/tas2_ir/rcp60/ssp5.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/climate/tas/rcp85.nc>
    loading <Archive /GCP/socioeconomics/popop/ssp1.nc>
    <Archive /GCP/climate/tas2_ir/rcp85/ssp1.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp2.nc>
    <Archive /GCP/climate/tas2_ir/rcp85/ssp2.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp3.nc>
    <Archive /GCP/climate/tas2_ir/rcp85/ssp3.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp4.nc>
    <Archive /GCP/climate/tas2_ir/rcp85/ssp4.nc> bumped 0.0.1 --> 0.0.2
    loading <Archive /GCP/socioeconomics/popop/ssp5.nc>
    <Archive /GCP/climate/tas2_ir/rcp85/ssp5.nc> bumped 0.0.1 --> 0.0.2


nifty!
