from thunder.streaming.site.configurations import NicksFeederConf

NicksFeederConf.set_image_prefix("images")
NicksFeederConf.set_behaviors_prefix("behaviour")
NicksFeederConf.set_images_dir("/groups/freeman/freemanlab/Streaming/anm_0245496_2015_02_27_run_04/registered_im")
NicksFeederConf.set_behaviors_dir("/groups/freeman/freemanlab/Streaming/anm_0245496_2015_02_27_run_04/registered_bv")

CHECKPOINT_DIR = "/nobackup/freeman/checkpoint/" 
READ_LOCATION = "/nobackup/freeman/streaminginput/" 
TEXT_WRITE_LOCATION = "/groups/freeman/home/osheroffa/series_text_output/"
BIN_WRITE_LOCATION = "/nobackup/freeman/streamingoutput_2015_03_02/"

analysis1 = Analysis.SeriesMeanAnalysis(data_path=READ_LOCATION, format="binary")
output1 = Output.SeriesFileOutput(directory=BIN_WRITE_LOCATION, prefix="output", format="binary", include_keys="false")
#output2 = Output.SeriesFileOutput(directory=TEXT_WRITE_LOCATION, prefix="output", format="text", 
#		include_keys="true")

tssc.set_checkpoint_dir(CHECKPOINT_DIR)
tssc.set_master("spark://h08u15.int.janelia.org:7077")
tssc.set_feeder_conf(NicksFeederConf) 

analysis1.add_output(output1)
tssc.add_analysis(analysis1)

NicksFeederConf.set_poll_time(5) 
NicksFeederConf.set_linger_time(-1)
