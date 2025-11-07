""" Converts ARTMIP contributions to a format that conforms to the ARTMIP standard """
import os
import glob
import dask.config
import xarray as xr
import collections
import inspect
import artmip_corrections
from dask.diagnostics import ProgressBar
import dask

# force dask to use a single thread
# (this avoids an HDF5 corruption issue)
dask.config.set(scheduler='threads')
dask.config.set(num_workers=1)


class ARTMIPStandardizer:

    def __init__(
        self,
        artmip_input_files,
        original_input_files,
        output_file_template,
        compression_level = 4,
        auto_load_files = True,
        auto_apply_corrections = True,
        auto_write_files = True,
        metadata_dict = None,
        artmip_metadata_dict = None,
        ar_tag_fill_value = None,
        decode_files_separately = False,
        be_verbose = True,
    ):
        """ Initialize the ARTMIPStandardizer class. 

        input:
        ------

            artmip_input_files     : the ARMTIP files to process; this argument
                                     should be compatible with 
                                     `xarray.open_mfdataset()`.
            
            original_input_files   : the original dataset on which ARMIP ARDTs 
                                     were run; this argument should be compatible with `xarray.open_mfdataset()`.
            
            
            output_file_template   : a string format template for the output 
                                   files.  Must contain the template field 
                                   `{year}`.
            
            compression_level      : the compression level for the files

            auto_load_files        : flags whether to automatically load 
                                     input datasets
                                     
            auto_apply_corrections : flags whether to automatically apply 
                                     corrections to the input dataset

            auto_write_files       : flags whether to automatically write 
                                     the output files to disk

            decode_files_separately : flags whether to decode each input
                                        file separately (for files with different time units)

            be_verbose             : flags whether to be verbose

            metadata_dict          : a dictionary of metadata values to add to the IVT input files

            artmip_metadata_dict   : a dictionary of metadata values to add to the ARTMIP input files

            ar_tag_fill_value      : the fill value for ar_binary_tag
                                     If None is given, no fill value is used.

        """

        # initalize the corrections to apply
        self.corrections = collections.OrderedDict()

        # initialize the description of corrections that will be applied
        self.correction_descriptions = collections.OrderedDict()

        # initialize the input xarray datasets
        self.artmip_input_xr = None
        self.original_input_xr = None

        # initialize the output xarray dataset
        self.output_xr = None

        # store input arguments
        self.artmip_input_files = artmip_input_files
        self.original_input_files = original_input_files
        self.output_file_template = output_file_template
        self.compression_level = compression_level
        self.auto_apply_corrections = auto_apply_corrections
        self.metadata_dict = metadata_dict
        self.artmip_metadata_dict = artmip_metadata_dict
        self.auto_load_files = auto_load_files
        self.ar_tag_fill_value = ar_tag_fill_value
        self.auto_write_files = auto_write_files
        self.decode_files_separately = decode_files_separately
        self.be_verbose = be_verbose

        if self.auto_load_files:
            # read the ARTMIP input files
            self.load_artmip_input_files()

            # read the original input files
            self.load_original_input_files()

        if self.auto_apply_corrections:
            # determine corrections to apply
            self.determine_corrections()

            # apply the corrections
            self.apply_corrections()

        if self.auto_write_files:
            self.write_dataset()

        return

    def load_artmip_input_files(self):
        """ Loads the ARTMIP input files; stores as self.artmip_input_xr """

        if not self.decode_files_separately:
            self.artmip_input_xr = xr.open_mfdataset(
                self.artmip_input_files,
                decode_times = False,
                data_vars = 'all',
                combine = 'nested',
                concat_dim='time')

            # if we were given metadata, apply it to the artmip dataset
            for var, att_dict in self.artmip_metadata_dict.items():
                for att, val in att_dict.items():
                    self.artmip_input_xr[var].attrs[att] = val
        else:
            if isinstance(self.artmip_input_files,str):
                file_list = sorted(glob.glob(self.artmip_input_files))
            # check if a list was given
            elif isinstance(self.artmip_input_files, list):
                file_list = self.artmip_input_files
            else:
                raise RuntimeError(f"artmip_input_files must be a glob string or a list of file paths when decode_files_separately is True. A {type(self.artmip_input_files)} was given.")

            # load and decode each file separately
            xr_list = []
            for filepath in file_list:
                tmp_xr = xr.open_dataset(
                    filepath,
                    decode_times = False,
                    )
                # if we were given metadata, apply it to the artmip dataset
                for var, att_dict in self.artmip_metadata_dict.items():
                    for att, val in att_dict.items():
                        tmp_xr[var].attrs[att] = val

                # decode the time values
                tmp_xr = xr.decode_cf(tmp_xr)

                xr_list.append(tmp_xr)

            # combine the list of xarrays
            self.artmip_input_xr = xr.concat(
                xr_list,
                dim = 'time',
                )


    def load_original_input_files(self):
        """ Loads the original input files; stores as self.original_input_xr """
        self.original_input_xr = xr.open_mfdataset(
            self.original_input_files,
            data_vars = 'all',
            decode_times = False,
            combine = 'nested',
            concat_dim='time')

        # if we were given metadata, apply it to the artmip dataset
        for var, att_dict in self.metadata_dict.items():
            for att, val in att_dict.items():
                self.original_input_xr[var].attrs[att] = val




    def determine_corrections(self):
        """ Generates a list of corrections to run on the ARTMIP dataset. """

        for func_name, correction_func in \
            artmip_corrections.all_corrections.items():

            # run through each possible correction
            # and determine if it should be applied
            if correction_func(
                artmip_xr = self.artmip_input_xr,
                input_xr = self.original_input_xr,
                determine_only = True) \
                    == True:

                self.__add_correction_to_list__(
                    func_name,
                    correction_func,
                    artmip_corrections.all_correction_descriptions[func_name],
                    )


    def apply_corrections(self):
        """ Applies the corrections to the ARTMIP dataset. """

        # set the current xarray dataset
        current_xr = self.artmip_input_xr

        for correction, correction_func in \
            self.corrections.items():

            # run through each correction in the list
            # and apply it
            output_xr = correction_func(
                artmip_xr = current_xr,
                input_xr = self.original_input_xr,
                apply_only = True)

            # use this xarray dataset for the next correction
            current_xr = output_xr

        self.output_xr = output_xr


    def __add_correction_to_list__(self, func_name, func, func_desc = ""):
        # check the correction function validity
        artmip_corrections.correction(func, add_to_list = False)

        # add this function to the list of corrections
        self.corrections[func_name] = func

        # add this function's documentation to the list of corrections
        self.correction_descriptions[func_name] = func_desc

    def write_dataset(self):
        """ Write the dataset to the path specified by self.output_file_template"""

        if self.output_xr is None:
            raise RuntimeError("Need to run apply_corrections() prior to writing.")

        output_xr = self.output_xr

        # add metadata about the operations performed
        output_xr.attrs["quality_control_operations"] = \
            "; ".join([desc.strip() for f, 
                desc in self.correction_descriptions.items()])

        # set netCDF options like compression and fill value
        encoding_dict = {}
        for var in output_xr.variables:
            encoding_dict[var] = \
                {
                    'zlib': True,
                    'complevel' : 4,
                    '_FillValue' : None,
                }

        # force the time units to be the same for all files
        encoding_dict["time"]["units"] = \
            str(output_xr['time'].attrs["units"])
        encoding_dict["time"]["calendar"] = \
            str(output_xr['time'].attrs["calendar"])


        # ensure that the coordinate data types match the input coordinate data types
        for var in ["time", "lat", "lon"]:
            encoding_dict[var]['dtype'] = str(self.original_input_xr[var].dtype)

        # ensure that ar_binary_tag has byte type
        encoding_dict['ar_binary_tag'] = dict(dtype="int8")

        # use mfdataset to write to disk
        output_xr = xr.decode_cf(output_xr) # decode to get the time values

        # group datasets by year
        years, datasets = zip(*output_xr.groupby("time.year"))
        paths = [ self.output_file_template.format(year = y) for y in years]

        # make the directories
        for path in paths:
            os.makedirs(os.path.dirname(path), exist_ok = True)

        # write to disk
        delayed_write = xr.save_mfdataset(
            datasets,
            paths,
            compute = False,
            encoding = encoding_dict,
            unlimited_dims = "time",
            )
        if self.be_verbose:
            with ProgressBar():
                results = delayed_write.compute()
        else:
            results = delayed_write.compute()



        

if __name__ == "__main__":

    time_override_dict = dict(
        time = {
            "long_name" : "time",
            "units" : "days since 0001-01-01 00:00:00",
            "calendar" : "365_day",
            "standard_name" : "time",
        },
        lat = {
            "long_name" : "latitude",
            "units" : "degrees_north",
            "standard_name" : "latitude",
        },
        lon = {
            "long_name" : "longitude",
            "units" : "degrees_east",
            "standard_name" : "longitude",
        },
    )
        #artmip_input_files="/N/scratch/obrienta/ARTMIP Tier 2 Paleo/Mundhenk_v3/PreIndust/*.nc4",
        #artmip_input_files="/N/scratch/obrienta/PaleoARTMIP/PaleoARTMIP_teca_bard_v1.0.1/PreIndust/PreIndust.ar_tag.teca_bard_v1.0.1.6hr.*.nc4",
#    dum = ARTMIPStandardizer(
#        artmip_input_files="/N/scratch/obrienta/ARTMIP Tier 2 Paleo/IDL/PreIndust.ar_tag.IDL_v2b.perc_PreIndust*.nc4",
#        original_input_files="/N/scratch/obrienta/PaleoARTMIP/PreIndust/IVT.cam.h2.01*.nc",
#        output_file_template="/N/scratch/obrienta/PaleoARTMIP/standardization_testing/IDL_v2b.perc_PreIndust/PreIndust/PreIndust.ar_tag.IDL_v2b.perc_PreIndust.6hr.{year:04}.nc4",
#        metadata_dict = time_override_dict,
#        )
#
#    dum = ARTMIPStandardizer(
#        artmip_input_files="/N/scratch/obrienta/ARTMIP Tier 2 Paleo/shields/PreIndust.ar_tag.Shields_v1.6hr.*.nc4",
#        original_input_files="/N/scratch/obrienta/PaleoARTMIP/PreIndust/IVT.cam.h2.01*.nc",
#        output_file_template="/N/scratch/obrienta/PaleoARTMIP/standardization_testing/Shields_v1/PreIndust/PreIndust.ar_tag.Shields_v1.6hr.{year:04}.nc4",
#        metadata_dict = time_override_dict,
#        )

    dum = ARTMIPStandardizer(
        artmip_input_files="/N/scratch/obrienta/ARTMIP Tier 2 Paleo//Guan_Waliser/Paleo/PreIndust*ar_tag*.nc4",
        original_input_files="/N/scratch/obrienta/PaleoARTMIP/PreIndust/IVT.cam.h2.01*.nc",
        output_file_template="/N/scratch/obrienta/PaleoARTMIP/standardization_testing/Mundhenk_v3/PreIndust/PreIndust.ar_tag.Guan_Waliser_v2.6hr.{year:04}.nc4",
        metadata_dict = time_override_dict,
        )