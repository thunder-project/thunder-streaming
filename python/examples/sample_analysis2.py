tsc.set_checkpoint_dir("/Users/osheroffa/Work/sample_data/checkpoint/")

analysis1 = Analysis.SeriesCountAnalysis(data_path="/Users/osheroffa/Work/development/sample_data/data", format="text")
output1 = Output.SeriesFileOutput(directory="/Users/osheroffa/Work/development/sample_data/series_output/", prefix="counts", format="text", include_keys="true")

analysis2 = Analysis.SeriesMeanAnalysis(data_path="/Users/osheroffa/Work/development/sample_data/data", format="text")
output2 = Output.SeriesFileOutput(directory="/Users/osheroffa/Work/development/sample_data/series_output/", prefix="means", format="text", include_keys="true")

analysis1.add_output(output1)
analysis2.add_output(output2)

tsc.add_analysis(analysis1)
tsc.add_analysis(analysis2)

tsc.set_master("local[20]")
tsc.start()
