""" Standardizes files in the ARTMIP Tier 2 Paleo experiment. """
import glob
from ARTMIPStandardizer import ARTMIPStandardizer
import argparse
import simplempi.simpleMPI as simpleMPI

# initialize MPI
smpi = simpleMPI.simpleMPI()

def vprint(*args, **kwargs):
    if smpi.rank == 0:
        print(*args, **kwargs)

# define the time, lat, and, lon attributes to enforce
# for all files
coord_override_dict = dict(
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

# set the experiments
experiments=["PreIndust", "PI_21ka-CO2", "10ka-Orbital"]
algorithms=[ \
    "ARCONNECT_v2",
    "Brands_v1.1",
    "IDL_v2b.perc_PreIndust",
    "IDL_v2b.perc_PI_21ka-CO2",
    "IDL_v2b.perc_10ka-Orbital",
    "IPART_v1",
    "Lora_v2",
    "Mundhenk_v3",
    "Reid250",
    "Reid500",
    "Shields_v1",
    "teca_bard_v1.0.1",
    "TE_v2.1",
    "Guan_Waliser_v2",
    ]

def list_algs_and_quit():
    for alg in algorithms:
        vprint(alg)
    quit()

def list_experiments_and_quit():
    for exp in experiments:
        vprint(exp)
    quit()

# parse the command line options
parser = argparse.ArgumentParser()
parser.add_argument("--algs", default = None, nargs='+',
    help="Algorithm(s) to run on.")
parser.add_argument("--exps", default = None, nargs='+',
    help="Experiment(s) to run on.")
parser.add_argument("--list_algs", default = False, action="store_true",
    help="Lists all valid algorithms")
parser.add_argument("--list_exps", default = False, action="store_true",
    help="Lists all valid experiments")
args = parser.parse_args()
parser_algs = args.algs
parser_exps = args.exps
if args.list_algs:
    list_algs_and_quit()
if args.list_exps:
    list_experiments_and_quit()
# get the list of algorithms to run on
if parser_algs is not None:
    # check that algorithms are valid
    for alg in parser_algs:
        assert alg in algorithms, f"Algorithm `{alg}` is not in the list of valid algorithms"
    # use this list of algorithms
    algorithms = parser_algs

# get the list of experiments to run on
if parser_exps is not None:
    # check that experiments are valid
    for exp in parser_exps:
        assert exp in experiments, f"Experiment `{exp}` is not in the list of valid experiments"
    # use this list of experiments
    experiments = parser_exps


# note: the above list is alphabetical, except that GW_v2 is at the bottom
# because it takes substantially longer than the others due to level 9
# compression having been used.

input_base = "/N/scratch/obrienta/ARTMIP Tier 2 Paleo/"
output_base = "/N/scratch/obrienta/PaleoARTMIP/standardized_dataset/"
input_file_glob_template = "/N/scratch/obrienta/PaleoARTMIP/{experiment}/IVT.cam.h2.*.nc"
output_file_template = output_base + "/{algorithm}/{experiment}/{experiment}.ar_tag.{algorithm}.6hr.{{year:04}}.nc4"


input_paths = {alg : {} for alg in algorithms}
for alg in algorithms:
    # initialize the input path using the file layout that several experiments
    # use
    glob_template = f"{input_base}/{alg}/{{experiment}}/*ar_tag*.nc4"
    
    for experiment in experiments:

        # set the default glob pattern
        glob_pattern = glob_template.format(experiment = experiment)

        # fix issues with individual algorithm layouts
        if alg == "IPART_v1":
            exp_tmp = experiment
            if experiment == "10ka-Orbital":
                # fix a spelling problem
                exp_tmp = "10ka_Orbital"
            glob_pattern = f"{input_base}/IPART/{exp_tmp}/*ar_tag*.nc4"

        if alg == "TE_v2.1":
            exp_tmp = experiment
            if experiment == "10ka-Orbital":
                # fix a spelling problem
                exp_tmp = "10ka-Orbitak"
            glob_pattern = f"{input_base}/Tempest/{exp_tmp}/*ar_tag*.nc4"

        if alg == "Shields_v1":
            glob_pattern = f"{input_base}/shields/{experiment}*ar_tag*.nc4"

        if alg == "Brands_v1.1":
            glob_pattern = f"{input_base}/Brands/brands_v1.1/{experiment}/*ar_tag*.nc4"

        if alg == "Guan_Waliser_v2":
            glob_pattern = f"{input_base}/Guan_Waliser/Paleo/{experiment}*ar_tag*.nc4"

        if "Reid" in alg:
            glob_pattern = f"{input_base}/Reid/{experiment}/*ar_tag.{alg}.*.nc4"

        if alg[:3] == "IDL":
            glob_pattern = f"{input_base}/IDL/{experiment}.ar_tag.{alg}*.nc4"

        nfiles = len(glob.glob(glob_pattern))

        # try a flat layout if none were found after all the modifications above
        if nfiles == 0:
            glob_pattern = f"{input_base}/{alg}/{experiment}*ar_tag*.nc4"
            nfiles = len(glob.glob(glob_pattern))

        # test again
        nfiles = len(glob.glob(glob_pattern))
        assert nfiles > 0, \
            f"Algorithm `{alg}`, experiment `{experiment}` has no files."

        # store the glob pattern
        input_paths[alg][experiment] = glob_pattern
        
        vprint(f"{alg}\t{experiment}\t{nfiles}")
vprint()

alg_exp_list = [ (exp, alg) for exp in experiments for alg in algorithms]
my_alg_exp_list = smpi.scatterList(alg_exp_list)

# loop over algorithms and experiments and run the standardizer
for exp, alg in my_alg_exp_list:
    glob_pattern = input_paths[alg][exp]

    # fix the time units for each experiment
    if exp == "10ka-Orbital":
        coord_override_dict["time"]["units"] \
            = "days since 0201-01-01 00:00:00"
    else:
        coord_override_dict["time"]["units"] \
            = "days since 0001-01-01 00:00:00"

    smpi.pprint(f"Standardizing {alg}:{exp}")
    ARTMIPStandardizer(
        glob_pattern,
        input_file_glob_template.format(experiment = exp),
        output_file_template=output_file_template.format(
            algorithm = alg, experiment = exp),
        metadata_dict = coord_override_dict,
        be_verbose=False,
    )

smpi.pprint("Done.")