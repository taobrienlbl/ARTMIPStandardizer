""" Standardizes files in the ARTMIP Tier 2 Polar experiment. """
import glob
from ARTMIPStandardizer import ARTMIPStandardizer
import argparse
import simplempi

# initialize MPI
smpi = simplempi.simpleMPI(useMPI = False)

def vprint(*args, **kwargs):
    if smpi.rank == 0:
        print(*args, **kwargs)

# define the time, lat, and, lon attributes to enforce
# for all files
coord_override_dict = dict(
    time = {
        "long_name" : "time",
        "calendar" : "noleap",
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
experiments=[ \
  "b.e21.BHISTsmbb.f09_g17.LE2-1011.001",
  "b.e21.BHISTsmbb.f09_g17.LE2-1031.002",
  "b.e21.BHISTsmbb.f09_g17.LE2-1051.003",
  "b.e21.BSSP370smbb.f09_g17.LE2-1011.001",
  "b.e21.BSSP370smbb.f09_g17.LE2-1031.002",
  "b.e21.BSSP370smbb.f09_g17.LE2-1051.003",
]

# set the algorithms
algorithms=[ \
  "Lora_v2",
  "Mattingly",
  "Mundhenk_v4",
  "PIKART_v1.1",
  "Wille",
  "Victoire_LE2_relative",
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


input_base = "/N/project/cascade/user_work_directories/obrienta/artmip/Tier2_Polar_CESM"
output_base = f"{input_base}/standardized"
artmip_file_glob_template = f"{input_base}/pre-standard/CESM2-LE_{{algorithm}}/{{experiment}}*ar_tag*.n*"
input_file_glob_template = f"{input_base}/IVT/{{experiment}}*.nc"
output_file_template = output_base + "/{algorithm}/{experiment}.ar_tag.{algorithm}.6hr.{{year:04}}010100-{{year:04}}123118.nc4"


input_paths = {alg : {} for alg in algorithms}
for alg in algorithms:
    
    for exp in experiments:

        # set the default glob pattern
        glob_pattern = artmip_file_glob_template.format(algorithm = alg, experiment = exp)
        if alg == "Lora_v2":
          glob_pattern = artmip_file_glob_template.format(algorithm = "Lora_linked", experiment = exp)
        elif alg == "Mundhenk_v4":
          tmp = exp.replace("b.e21.","").replace("f09_g17.LE2-","")
          glob_pattern = artmip_file_glob_template.format(algorithm = alg, experiment = tmp)
 
        # find files
        nfiles = len(glob.glob(glob_pattern))
        assert nfiles > 0, \
            f"Algorithm `{alg}`, experiment `{exp}` has no files. \\ {glob_pattern}"

        # store the glob pattern
        input_paths[alg][exp] = glob_pattern
        
        vprint(f"{alg}\t{exp}\t{nfiles}")
vprint()

alg_exp_list = [ (exp, alg) for exp in experiments for alg in algorithms]
my_alg_exp_list = smpi.scatterList(alg_exp_list)

# loop over algorithms and experiments and run the standardizer
for exp, alg in my_alg_exp_list:
    glob_pattern = input_paths[alg][exp]


    # fix the time units for each experiment
    artmip_coord_override_dict = {}
    if "BSSP370" in exp:
        coord_override_dict["time"]["units"] = "days since 2015-01-01 00:00:00"
    else:
        coord_override_dict["time"]["units"] = "days since 1850-01-01 00:00:00"

    decode_files_separately = False

    forced_time_range = None
    if alg == "Mattingly":
        # force the times in this algorithm to come from the input dataset
        # due to decoding issues with the time coordinate
        if "BHIST" in exp:
            forced_time_range = ("1990-01-01 00:00:00", "2009-12-31 18:00:00")
        if "BSSP370" in exp:
            forced_time_range = ("2080-01-01 00:00:00", "2094-12-31 18:00:00")

    smpi.pprint(f"Standardizing {alg}:{exp}")
    ARTMIPStandardizer(
        glob_pattern,
        input_file_glob_template.format(experiment = exp),
        output_file_template=output_file_template.format(
            algorithm = alg, experiment = exp),
        metadata_dict = coord_override_dict,
        artmip_metadata_dict = artmip_coord_override_dict,
        decode_files_separately = decode_files_separately,
        be_verbose = False,
        forced_time_range = forced_time_range,
    )

smpi.pprint("Done.")
