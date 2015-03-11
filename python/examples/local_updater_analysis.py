from thunder.streaming.site.configurations import NikitasFeederConf
from thunder.streaming.shell.examples.example_updater import ExampleUpdater
import os
import glob
import math

SAMPLE_DIR = "~/Work/development/sample_data/streaming_test/" 

dirs = {
    "checkpoint": os.path.join(SAMPLE_DIR, "checkpoint"),
    "input": os.path.join(SAMPLE_DIR, "streaminginput"),
    "output": os.path.join(SAMPLE_DIR, "streamingoutput"),
    "images": os.path.join(SAMPLE_DIR, "images"),
    "behaviors": os.path.join(SAMPLE_DIR, "behaviors"),
    "temp": os.path.join(SAMPLE_DIR, "temp")
}

run_params = { 
    "checkpoint_interval": 10000, 
    "hadoop_block_size": 1, 
    "parallelism": 10, 
    "master": "local[2]",
    "batch_time": 20
}

feeder_params = { 
    "linger_time": -1, 
    "max_files": 10, 
    "poll_time": 5
}

test_data_params = { 
    "prefix": "input_",
    "num_files": 50,
    "approx_file_size": 100 ,
    "records_per_file": 50000
}

##########################################
# Analysis configuration stuff starts here
##########################################

analysis1 = Analysis.SeriesMeanAnalysis(input=dirs['input'], output=dirs['output'], prefix="output", format="binary") 
tssc.add_analysis(analysis1)

updaters = [
    ExampleUpdater(tssc, analysis1.identifier),
]

########################################
# Analysis configuration stuff ends here
########################################

# Attach all the parameters in the dictionary aboves to their respective objects
def attach_parameters(): 
    for key, value in run_params.items():
        tssc.__dict__['set_'+key](value)
    tssc.set_checkpoint(dirs['checkpoint'])

# Create the directories if they don't exist, clear them if they do
def set_up_directories(): 
    for directory in dirs.values(): 
        if not os.path.exists(directory): 
            os.makedirs(directory)
        else: 
            files = glob.glob(os.path.join(directory, "*"))
            for f in files: 
                os.remove(f)

# Populate the images/behaviors directories with test data 
def generate_test_series(dirs): 
    def write_file(directory, i): 
        file_path = os.path.join(directory, test_data_params['prefix'] + str(i))
        with open(file_path, 'w') as output_file: 
            approx_size = float(test_data_params['approx_file_size'] * 1000000)
            series_len = int((approx_size / test_data_params['records_per_file']) / 8.0) - 1 
            for j in xrange(test_data_params['records_per_file']): 
                for k in xrange(series_len):
                    output_file.write('%.df ' % (random.random() * 10))
                output_file.write('\n')
    for directory in dirs: 
        [write_file(directory, i) for i in xrange(test_data_params['num_files'])]

def generate_raw_test_data(): 
    pass

def make_feeder(): 
    pass
                    
def run(with_feeder=False): 
    attach_parameters()
    set_up_directories() 
    for updater in updaters: 
        updater.start()
    if with_feeder: 
        feeder = make_feeder()
        tssc.set_feeder_conf(feeder)
        tssc.start()
    else: 
        generate_test_series(list(dirs['temp']))
        tssc._start_streaming_child()
