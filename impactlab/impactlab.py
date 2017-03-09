
from functools import reduce


def uses(**kwargs):
    '''
    Specify variables that will be provided as keyword arguments to the function

    Parameters
    ----------

    kwargs : dict
        variable name, api.Variable object pair
        variable name will be supplied as a keyword argument to the function
        api.Variable will be iterated over for each element in its SuperIndex
        and the underlying archive will be provided as a value for each index
        value
    '''

    def decorator(func):
        def inner(**variables):
            for varname, var in kwargs.items():
                variables.update({varname: var})

            return func(**variables)
        return inner
    return decorator


def updates(update_var):
    '''
    Set the variable that a function's output will update
    '''

    def decorator(func):
        def inner(**variables):

            indices = {}
            for vname, v in variables.items():
                if hasattr(v, 'indices'):
                    indices.update(v.indices)

            res = func(**variables)

            archive = update_var.get_archive(**indices)
            archive.update(res)

        return inner
    return decorator


def iters():
    def decorator(func):
        def inner(**variables):

            # Create a superset of the superindices to iterate over
            master_index_list = reduce(lambda x, y: x*y, (v.superindex for v in variables.values() if hasattr(v, 'superindex')))
            for indices in master_index_list:

                sliced_vars = {vname: v.get_archive(**indices) if hasattr(v, 'superindex') else v for vname, v in variables.items()}

                func(**sliced_vars)

        return inner
    return decorator
