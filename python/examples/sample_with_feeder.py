from thunder.streaming.site.configurations import NicksFeederConf

CHECKPOINT_DIR = "/nobackup/freeman/checkpoint/"
READ_LOCATION = "/nobackup/freeman/streaminginput/"
TEXT_WRITE_LOCATION = "/groups/freeman/home/osheroffa/series_text_output/"
BIN_WRITE_LOCATION = "/groups/freeman/freemanlab/streamingoutput_2015_02_27/"
MASTER = "spark://h07u17.int.janelia.org:7077"

NicksFeederConf.set_image_prefix("images")
NicksFeederConf.set_behaviors_prefix("behaviour")
NicksFeederConf.set_images_dir("/groups/freeman/freemanlab/Streaming/anm_0245496_2015_02_27_run_04/registered_im")
NicksFeederConf.set_behaviors_dir("/groups/freeman/freemanlab/Streaming/anm_0245496_2015_02_27_run_04/registered_bv")

analysis1 = Analysis.SeriesMeanAnalysis(data_path=READ_LOCATION, format="binary")
output1 = Output.SeriesFileOutput(directory=BIN_WRITE_LOCATION, prefix="output", format="binary",                
                include_keys="false")
output2 = Output.SeriesFileOutput(directory=TEXT_WRITE_LOCATION, prefix="output", format="text",
                include_keys="true")

tsc.set_checkpoint_dir(CHECKPOINT_DIR)
tsc.set_master(MASTER)
tsc.set_feeder_conf(NicksFeederConf)

analysis1.add_output(output1, output2)
tsc.add_analysis(analysis1)

NicksFeederConf.set_poll_time(5)
NicksFeederConf.set_linger_time(-1)
