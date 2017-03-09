
import itertools
import pandas as pd
import numpy as np
import io
from collections import OrderedDict
from datafs.core.versions import BumpableVersion
from contextlib import contextmanager

def _merge_dicts(dicts):
    """Given dicts, merge them into a new dict as a shallow copy."""
    z = {}
    for d in dicts:
        z.update(d)
    return z


class SuperIndex(object):
    '''
    Examples
    --------

    .. code-block:: python

        >>> rcp = SuperIndex(
        ...     'rcp',
        ...     'Representative Concentration Pathways',
        ...     [{'rcp': 'rcp{}'.format(r)} for r in [26, 45, 60, 85]])
        >>> rcp
        <SuperIndex rcp>
        >>> ssp = SuperIndex(
        ...     'ssp',
        ...     'Shared Socioeconomic Pathway',
        ...     [{'ssp': 'ssp{}'.format(s)} for s in range(1,6)])
        >>> ssp
        <SuperIndex ssp>
        >>> combined = rcp*ssp
        >>> combined
        <SuperIndex rcp*ssp>
        >>> list(combined) # doctest: +ELLIPSIS
        [{'rcp': 'rcp26', 'ssp': 'ssp1'}, ..., {'rcp': 'rcp85', 'ssp': 'ssp5'}]
        >>> filtered = combined[{'rcp': 'rcp85'}]
        >>> list(filtered) # doctest: +ELLIPSIS
        [{'rcp': 'rcp85', 'ssp': 'ssp1'}, ..., {'rcp': 'rcp85', 'ssp': 'ssp5'}]

    A SuperIndex may be created using either ``values`` or ``components``
    arguments, but not both:

    .. code-block:: python

        >>> SuperIndex('ind', 'my index', values=[{'ind': 1}], components={}) # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        ValueError: May provide values or components but not both


    A blank index may be created to simulate SuperIndex behavior:

    .. code-block:: python

        >>> blank = SuperIndex('blank', 'my blank index')
        >>> list(blank)
        []
        >>> rcp = SuperIndex(
        ...     'rcp',
        ...     'Representative Concentration Pathways',
        ...     [{'rcp': 'rcp{}'.format(r)} for r in [26, 45, 60, 85]])
        >>> blank*rcp
        <SuperIndex rcp>

    '''

    def __init__(self, name='empty', description=None, values=None, components=None):
        self.name = name
        self.description = description
        if (components is None) and (values is None):
            self.components = OrderedDict()
        elif components is None:
            self.components = OrderedDict([(name,  values)])
        elif values is None:
            self.components = components
        else:
            raise ValueError('May provide values or components but not both')

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.name)

    def __mul__(self, other):
        new_components = OrderedDict()
        new_components.update(self.components)
        new_components.update(other.components)

        new_name = '*'.join(map(str, new_components.keys()))

        return self.__class__(
            new_name,
            'combinatorial product of ({})'.format(new_name),
            components=new_components)

    def __iter__(self):
        if len(self.components) > 0:
            for x in itertools.product(*tuple(self.components.values())):
                yield _merge_dicts(x)

    def __getitem__(self, key):
        assert hasattr(key, '__getitem__'), "SuperIndex needs a dictionary for slicing"
        
        new_components = OrderedDict()
        for component in self.components.keys():
            if component in key:
                new_components[component] = [{component: key[component]}]
            elif component in self.components:
                new_components[component] = self.components[component]

        return self.__class__(
            '{}'.format(self.name),
            '({}) sliced with {}'.format(self.name, key),
            components=new_components)


class Archive(object):
    '''
    Example
    -------

    .. code-block:: python

        >>> arch = Archive('/climate/raw', rcp='rcp85')
        >>> arch.update(5)
        <Archive /climate/raw/rcp85.nc> bumped 0.0.1 --> 0.0.2
        >>> arch.update(3)
        <Archive /climate/raw/rcp85.nc> bumped 0.0.2 --> 0.0.3

    '''
    
    def __init__(self, name, indices):
        self.name = name
        self.indices = indices
        self.version = BumpableVersion('0.0.1')
        self.value = pd.DataFrame(np.random.random((5,4)))

    def __repr__(self):
        return '<{} {}.nc>'.format(self.__class__.__name__, self.name)

    def update(self, value):
        self.value = value
        old = str(self.version)
        self.version.bump('patch', inplace=True)
        print('{} bumped {} --> {}'.format(str(self), old, self.version))

    @contextmanager
    def open(self, *args, **kwargs):
        print('loading {}'.format(self))
        s = io.BytesIO()
        self.value.to_csv(s)
        s.seek(0, 0)
        yield s

class Variable(object):
    '''
    Examples
    --------

    .. code-block:: python

    >>> rcp = SuperIndex(
    ...     'rcp',
    ...     'Representative Concentration Pathways', 
    ...     [{'rcp': 'rcp{}'.format(r)} for r in [26, 45, 60, 85]])
    >>> ssp = SuperIndex(
    ...     'ssp',
    ...     'Shared Socioeconomic Pathways',
    ...     [{'ssp': 'ssp{}'.format(s)} for s in range(1,6)])
    >>> scalar = Variable('my5')
    >>> pop = Variable('mypop', superindex=ssp)
    >>> tas = Variable('temperature', superindex=rcp)
    >>> mort = Variable('mortality', superindex=ssp*rcp)
    >>> list(mort.superindex)
    [{'rcp': 'rcp26', 'ssp': 'ssp1'}, ..., {'rcp': 'rcp85', 'ssp': 'ssp5'}]
    >>> mort_rcp85 = mort[{'rcp': 'rcp85'}]
    >>> list(mort_rcp85.superindex)
    [{'rcp': 'rcp85', 'ssp': 'ssp1'}, ..., {'rcp': 'rcp85', 'ssp': 'ssp5'}]

    '''

    def __init__(self, name, superindex=None, api=None):
        self.name = name

        if superindex is None:
            superindex = SuperIndex()

        self.superindex = superindex
        self.api = api

    def __getitem__(self, key):
        return self.__class__(self.name, self.superindex[key], api=self.api)

    def get_archive(self, **indices):
        return self.api.get_archive(self.name, indices)


class DataAPI(object):

    def __init__(self):
        self.superindices = {
            'rcp': SuperIndex('rcp', 'Representative Concentration Pathways', [{'rcp': 'rcp{}'.format(r)} for r in [26, 45, 60, 85]]),
            'ssp': SuperIndex('ssp', 'Shared Socioeconomic Pathway', [{'ssp': 'ssp{}'.format(s)} for s in range(1,6)])
        }

        variables = [
            Variable('/GCP/socioeconomics/popop', self.superindices['ssp'], api=self),
            Variable('/GCP/climate/tas', self.superindices['rcp'], api=self),
            Variable('/GCP/impacts/mortality', self.superindices['rcp']*self.superindices['ssp'], api=self),
            Variable('/GCP/climate/tas2_ir', self.superindices['rcp'], api=self)]

        self.variables = {v.name: v for v in variables}

        self.archives = {}

    def get_variable(self, var):
        return self.variables[var]

    def get_archive(self, var, indices):

        arch_name = var + '/'.join(map(lambda x: str(x[1]), sorted(indices.items(), key=lambda x: x[0]))) + '.nc'

        if arch_name not in self.archives:
            self.archives[arch_name] = Archive(arch_name, indices)

        return self.archives[arch_name]
