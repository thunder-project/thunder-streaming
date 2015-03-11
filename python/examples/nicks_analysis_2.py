
NicksFeederConf.set_image_prefix("images")
NicksFeederConf.set_behaviors_prefix("behaviour")
NicksFeederConf.set_images_dir("/groups/freeman/freemanlab/Streaming/anm_0245496_2015_02_27_run_04/registered_im")
NicksFeederConf.set_behaviors_dir("/groups/freeman/freemanlab/Streaming/anm_0245496_2015_02_27_run_04/registered_bv")

CHECKPOINT_DIR = "/nobackup/freeman/checkpoint/" 
READ_LOCATION = "/nobackup/freeman/streaminginput/" 
TEXT_WRITE_LOCATION = "/groups/freeman/home/osheroffa/series_text_output/"
BIN_WRITE_LOCATION = "/nobackup/freeman/streamingoutput_2015_03_02/"

analysis1 = Analysis.SeriesMeanAnalysis(data_path=READ_LOCATION, format="binary")

tsc.set_checkpoint_dir(CHECKPOINT_DIR)
tsc.set_master("spark://h08u15.int.janelia.org:7077")
tsc.set_feeder_conf(NicksFeederConf) 

analysis1.add_output(output1)
tsc.add_analysis(analysis1)

NicksFeederConf.set_poll_time(5) 
NicksFeederConf.set_linger_time(-1)
