from thunder.streaming.shell.examples.lightning_updater import LightningUpdater
from lightning import Lightning
import numpy as np
from numpy import zeros 
import os
import glob
import math
import shutil
import random
import time
import signal

SAMPLE_DIR = "/groups/freeman/home/osheroffa/sample_data/" 

interrupted = False

int_handler = signal.getsignal(signal.SIGINT)
term_handler = signal.getsignal(signal.SIGTERM)

def new_handler(signum, stack): 
    global interrupted
    int_handler(signum, stack)
    interrupted = True

signal.signal(signal.SIGINT, new_handler) 
signal.signal(signal.SIGTERM, new_handler)

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
    "parallelism": 100, 
    "master": "spark://h07u02.int.janelia.org:7077",
    "batch_time": 10
}

feeder_params = { 
    "linger_time": -1, 
    "max_files": 10, 
    "poll_time": 5
}

test_data_params = { 
    "prefix": "input_",
    "num_files": 10,
    "approx_file_size": 10.0,
    "records_per_file": 512 * 512,
    "copy_period": 10
}

##########################################
# Analysis configuration stuff starts here
##########################################

# TODO Need to insert the Lightning client here
lgn = Lightning("http://kafka1.int.janelia.org:3000/")
lgn.create_session('test')

image_viz = lgn.image(zeros((512, 512)))
line_viz = lgn.linestreaming(zeros((10,1)))

analysis1 = Analysis.SeriesMeanAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'images'), prefix="output", format="text").toImage(dims=(512,512)).toLightning(image_viz, only_viz=True)
#analysis2 = Analysis.SeriesFiltering2Analysis(input=dirs['input'], output=os.path.join(dirs['output'], 'filtered_series'), prefix="output", format="text").toSeries().toLightning(line_viz, only_viz=True)

#analysis2.receive_updates(analysis1)

tssc.add_analysis(analysis1)
#tssc.add_analysis(analysis2)

updaters = [
    LightningUpdater(tssc, image_viz, analysis1.identifier)
]

for updater in updaters: 
    tssc.add_updater(updater)

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
            try: 
                for f in files: 
                    os.unlink(f)
            except Exception as e:
                print e

# Populate the images/behaviors directories with test data 
def generate_test_series(dirs): 
    def write_file(directory, i): 
        file_path = os.path.join(directory, test_data_params['prefix'] + str(i))
        print "Generating test series in %s..." % file_path
        with open(file_path, 'w') as output_file: 
            approx_size = float(test_data_params['approx_file_size'] * 1000000)
            series_len = int((approx_size / test_data_params['records_per_file']) / 8.0) - 1 
            for j in xrange(test_data_params['records_per_file']): 
                output_file.write('%d ' % j)
                for k in xrange(series_len):
                    output_file.write('%.2f ' % (random.random() * 10))
                output_file.write('\n')
    for directory in dirs: 
        [write_file(directory, i) for i in xrange(test_data_params['num_files'])]

# Copy data into the input directory at a certain rate 
def copy_data():
    copy_period = test_data_params['copy_period']
    num_files = test_data_params['num_files']
    for f in os.listdir(dirs['temp']): 
        print "Copying %s to input directory..." % f
        shutil.copy(os.path.join(dirs['temp'], f), dirs['input'])
        time.sleep(copy_period)
        if interrupted: 
            break

def generate_raw_test_data(): 
    pass

def make_feeder(): 
    pass

def run(with_feeder=False): 
    attach_parameters()
    set_up_directories() 
    if with_feeder: 
        feeder = make_feeder()
        tssc.set_feeder_conf(feeder)
    generate_test_series([dirs['temp']])
    tssc.start()
    copy_data()

