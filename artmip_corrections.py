""" Defines corrections that can be applied to ARTMIP data.

Each correction function should be defined using the `@correction` property,
which registers it as a correction (in the dictionary `all_corrections`), checks
that it's call signature has the expected inputs.

# Overview of `@correction` functions

Corrections have two phases: `determine_only` and `apply_only`.  These phases
correspond to boolean keyword arguments in a correction function, and they
define which phase is operating.

An example correction function, with the expected call signature, follows:

```python
@correction
def do_nothing(
    artmip_xr = None,
    input_xr = None,
    determine_only = False,
    apply_only = False,
):
'''Pass the ARTMIP dataset through without modification.'''

    if determine_only:
        # state the correction needs to be applied
        return True

    if apply_only:
        # apply the correction; in this case, just pass
        # the dataset through
        return artmip_xr

```

All correction functions should take four keyword arguments:

    artmip_xr : an `xarray.Dataset` represeting the ARTMIP dataset

    input_xr  : an `xarray.Dataset` represeting the dataset on which the ARTMIP
                algorithm was run.
    
    determine_only : flags whether the correction's determination phase should 
                     run

    apply_only     : flags whether the correction's apply phase should run

They should also have a short docstring that defines the action that the correction performs (as a verb-phrase).

## Determination phase

The `determine_only` phase inspects the `xarray.Dataset` object that represents
the ARTMIP dataset and possibly compares it to the `xarray.Dataset` object that
represents the input data on which the ARTMIP algorithm was run.  If the phase
determines that a correction needs to be made, then the function returns True;
it returns False otherwise.

## Apply phase

The `apply_only` phase applies the necessary correction to the the input dataset
and returns the resulting dataset.  An `xarray.Dataset` object should be
returned.
"""
import inspect
import collections
import numpy as np
import xarray as xr

# initialize the list of corrections and their descriptions
all_corrections = collections.OrderedDict()
all_correction_descriptions = collections.OrderedDict()

def correction(func, add_to_list = True):
    """ Register a function as an ARTMIP correction.
    
    Corrections have two phases: `determine_only` and `apply_only`.  These
    phases correspond to boolean keyword arguments in a correction function, and
    they define which phase is operating.
    
    An example correction function, with the expected call signature, follows:

    ```python
    @correction
    def do_nothing(
        artmip_xr = None,
        input_xr = None,
        determine_only = False,
        apply_only = False,
    ):
    '''Pass the ARTMIP dataset through without modification.'''

        if determine_only:
            # state the correction needs to be applied
            return True

        if apply_only:
            # apply the correction; in this case, just pass
            # the dataset through
            return artmip_xr

    ```

    All correction functions should take four keyword arguments:

        artmip_xr : an `xarray.Dataset` represeting the ARTMIP dataset

        input_xr  : an `xarray.Dataset` represeting the dataset on which the
                    ARTMIP algorithm was run.
        
        determine_only : flags whether the correction's determination phase
                         should run
        
        apply_only     : flags whether the correction's apply phase should run

    They should also have a short docstring that defines the action that the
    correction performs (as a verb-phrase).
    
    """
    # check that the function has the expected keyword arguments
    sig = inspect.signature(func)
    assert 'artmip_xr' in sig.parameters,\
        "Corrections must take an `artmip_xr` option."
    assert 'input_xr' in sig.parameters,\
        "Corrections must take an `input_xr` option."
    assert 'determine_only' in sig.parameters,\
        "Corrections must take an `determine_only` option."
    assert 'apply_only' in sig.parameters,\
        "Corrections must take an `apply_only` option."

    # check that the function has a docstring
    assert len(func.__doc__) > 0, "Corrections must supply a docstring"

    if add_to_list:
        # get the function name
        func_name = func.__name__

        # add the function to the master list
        all_corrections[func_name] = func

        # add the function's description to the master list
        all_correction_descriptions[func_name] = func.__doc__

    return func

# ******************************************************************************
# ******************************************************************************
# ************************** Correction functions ******************************
# ******************************************************************************
# ******************************************************************************

@correction
def swap_lon_convention(
    artmip_xr = None,
    input_xr = None,
    determine_only = False,
    apply_only = False,
    ):
    """ Swap the longitude convention from -180-180 or 0-360. """

    if determine_only:
        needs_correction = False

        # check if the input longitudes are identical
        if all(np.isclose(input_xr.lon.values, artmip_xr.lon.values)):
            # if they are, nothing needs to be done
            return False

        # figure out if the input dataset is 0-360 or -180-180
        if input_xr.lon.min() < 0 and input_xr.lon.max() > 0:
            input_is_0_360 = False
        else:
            input_is_0_360 = True

        # figure out if the artmip dataset is 0-360 or -180-180
        if artmip_xr.lon.min() < 0 and artmip_xr.max() > 0:
            artmip_is_0_360 = False
        else:
            artmip_is_0_360 = True

        # If they don't have the same convention, this correction needs to be
        # applied
        if input_is_0_360 != artmip_is_0_360:
            needs_correction = True
        else:
            # if they have the same convention but longtidues aren't identical,
            # something is wrong.
            raise RuntimeError(f"Longitudes aren't identical, but both datasets have the same longitude convention. artmip_xr.lon = {artmip_xr.lon}, input_xr.lon = {input_xr.lon}")
            
        # state whether correction needs to be applied
        return needs_correction

    if apply_only:

        # figure out if the artmip dataset is 0-360 or -180-180
        if artmip_xr.lon.min() < 0 and artmip_xr.max() > 0:
            artmip_is_0_360 = False
        else:
            artmip_is_0_360 = True

        # fix the longitudes (multiply by one to change from an immutable
        # indexarray to a mutable dataarray)
        lon = artmip_xr.lon
        if artmip_is_0_360:
            lon = xr.where(lon > 180, lon - 360, lon)
        else:
            lon = xr.where(lon < 0, lon + 360, lon)

        # replace the coordinate
        output_xr = artmip_xr.assign_coords(lon = lon)

        return output_xr

@correction
def rotate_longitudes(
    artmip_xr = None,
    input_xr = None,
    determine_only = False,
    apply_only = False
    ):
    """ Rotate through the longitude dimension to match the input dataset. """

    if determine_only:
        needs_correction = False

        # check if the input longitudes are identical
        if all(np.isclose(input_xr.lon, artmip_xr.lon)):
            # if they are, nothing needs to be done
            return False

        # determine the location of longitude 0 in the artmip dataset
        try:
            i0_artmip = np.nonzero(artmip_xr.lon.values == 0)[0][0]
        except:
            raise RuntimeError("Longitude 0 doesn't exist in ARTMIP dataset; it"
            "is not clear how to proceed")

        # determine the location of longitude 0 in the input dataset
        try:
            i0_input = np.nonzero(input_xr.lon.values == 0)[0][0]
        except:
            raise RuntimeError("Longitude 0 doesn't exist in input dataset; it"
            "is not clear how to proceed")
            
        nroll = i0_input - i0_artmip
        if nroll == 0:
            # if longitude 0 overlaps, nothing needs to be done
            return False

        # check if rolling the longitude fixes it
        rolled_lon = artmip_xr.lon.roll(lon = nroll)

        # if longitudes aren't identical, assume that 
        # the latlon convention will have to be changed
        if not all(np.isclose(input_xr.lon.values, rolled_lon.values)):
            # change from -180 to 180 to 0 to 360
            if rolled_lon.min() < 0:
                rolled_lon = xr.where(
                    rolled_lon < 0, rolled_lon + 360, rolled_lon)
            else:
                # change from 0 to 360 to -180 to 180
                rolled_lon = xr.where(
                    rolled_lon > 180, rolled_lon - 360, rolled_lon)

        # if rolling worked, we need the correction
        if all(np.isclose(input_xr.lon.values, rolled_lon.values)):
            needs_correction = True
        else:
            # if not, then something unexpected is going on
            raise RuntimeError(f"Longitudes aren't identical, but rolling lon didn't fix the problem. rolled_lon = {rolled_lon.values}, input_xr.lon = {input_xr.lon.values}")
            
        # state whether the correction needs to be applied
        return needs_correction

    if apply_only:
        # determine the location of longitude 0 in the artmip dataset
        try:
            i0_artmip = np.nonzero(artmip_xr.lon.values == 0)[0][0]
        except:
            raise RuntimeError("Longitude 0 doesn't exist in ARTMIP dataset; it"
            "is not clear how to proceed")

        # determine the location of longitude 0 in the input dataset
        try:
            i0_input = np.nonzero(input_xr.lon.values == 0)[0][0]
        except:
            raise RuntimeError("Longitude 0 doesn't exist in input dataset; it"
            "is not clear how to proceed")
            
        nroll = i0_input - i0_artmip

        # roll along the longitude dimension
        output_xr = artmip_xr.roll(lon = nroll, roll_coords = True)

        return output_xr

@correction
def insert_missing_times(
    artmip_xr = None,
    input_xr = None,
    determine_only = False,
    apply_only = False,
    ):
    """ Insert missing timesteps (fills with _FillValue). """
    if determine_only:
        needs_correction = False

        if len(input_xr.time) != len(artmip_xr.time):
            needs_correction = True

            # double-check that the output times are a superset of the inputs
            try:
                input_xr.sel(time = artmip_xr.time)
            except:
                raise RuntimeError("ARTMIP time values don't match those of the input; cannot determine how to sensibly modify the time dimension of the ARTMIP dataset.")

        return needs_correction

    if apply_only:

        # reindex the dataset to augment the times 
        output_xr = artmip_xr.reindex(
            dict(time = input_xr.time),
            fill_value = 0,
            copy = False)

        return output_xr

@correction
def override_time_values_and_metadata(
    artmip_xr = None,
    input_xr = None,
    determine_only = False,
    apply_only = False,
    ):
    """ Override the time values and metadata with that from the input. """

    if determine_only:
        needs_correction = False

        try:
            # check if the time values are all close
            if not np.allclose(input_xr.time.values, artmip_xr.time.values):
                needs_correction = True
        except ValueError:
            # the above might not work if the dataset is missing time values
            # but we are OK moving along here, since the insert_missing_times
            # correction checks that the times are a subset
            pass

        # check if the long name, units, calendar, and standard name all match
        check_atts = ["long_name", "units", "calendar", "standard_name"]
        for att in check_atts:
            try:
                # at least one of the attributes don't match; override them
                if input_xr['time'].attrs[att] != artmip_xr['time'].attrs[att]:
                    needs_correction = True
            except KeyError:
                # one of the attributes is missing; override them
                needs_correction = True

        return needs_correction

    if apply_only:
        # copy the time coordinate from the input dataset
        output_xr = artmip_xr.assign_coords(time = input_xr.time)

        return output_xr

@correction
def override_coordinate_metadata(
    artmip_xr = None,
    input_xr = None,
    determine_only = False,
    apply_only = False,
    ):
    """ Override the lat/lon coordinate metadata with that from the input. """
    check_coords = ["lat", "lon"]
    check_atts = ["long_name", "units", "standard_name"]

    if determine_only:
        needs_correction = False

        # check if the long name, units, and standard name all match
        for att in check_atts:
            for coord in check_coords:
                try:
                    # at least one of the attributes don't match; override them
                    if input_xr[coord].attrs[att] \
                        != artmip_xr[coord].attrs[att]:
                        needs_correction = True
                except KeyError:
                    # one of the attributes is missing; override them
                    needs_correction = True

        return needs_correction

    if apply_only:
        output_xr = artmip_xr.copy()
        # copy the coordinate metadata from the input dataset
        for coord in check_coords:
            for att in check_atts:
                output_xr[coord].attrs[att] = input_xr[coord].attrs[att]

        return output_xr