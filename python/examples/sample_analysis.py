
# Replace my strings with yours
CHECKPOINT_DIR = "/Users/osheroffa/Work/sample_data/checkpoint/"
READ_LOCATION = "/Users/osheroffa/Work/development/sample_data/data"
WRITE_LOCATION = "/Users/osheroffa/Work/development/sample_data/series_output/"
MASTER = "local[20]"

# This script creates one SeriesNoopAnalysis (which just sends the unmodified input to any 
# outputs) and write the output to a file using a SeriesFileOutput.

tsc.set_checkpoint_dir(CHECKPOINT_DIR)

analysis1 = Analysis.SeriesNoopAnalysis(data_path=READ_LOCATION, format="text")

output1 = Output.SeriesFileOutput(directory=WRITE_LOCATION, prefix="output", format="text", include_keys="true")

analysis1.add_output(output1)

tsc.add_analysis(analysis1)
tsc.set_master(MASTER)

tsc.start()
