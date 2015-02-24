tsc.set_checkpoint_dir("/Users/osheroffa/Work/sample_data/checkpoint/")

analysis1 = Analysis.SimpleExampleAnalysis(data_path="/Users/osheroffa/Work/development/sample_data/data", format="text")

output1 = Output.SeriesFileOutput(directory="/Users/osheroffa/Work/development/sample_data/series_output/", prefix="output", format="text", include_keys="true")
output2 = Output.SeriesFileOutput(directory="/Users/osheroffa/Work/development/sample_data/series_output/",prefix="output", format="binary", include_keys="true")

analysis1.add_output(output1, output2)

tsc.add_analysis(analysis1)
tsc.set_master("local[20]")
tsc.start()
