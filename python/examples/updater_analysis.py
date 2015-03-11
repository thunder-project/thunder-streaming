from thunder.streaming.site.configurations import NikitasFeederConf
from thunder.streaming.shell.examples.example_updater import ExampleUpdater

NikitasFeederConf.set_behaviors_dir("/nobackup/freeman/andrew/nikitatest/ephysSplitted/")
NikitasFeederConf.set_images_dir("/nobackup/freeman/andrew/nikitatest/raw/")
NikitasFeederConf.set_spark_input_dir("/tier2/freeman/streaming/streaminginput/")
NikitasFeederConf.set_tmp("/tier2/freeman/streaming/streaminginput_temp/")

CHECKPOINT_DIR = "/nobackup/freeman/checkpoint/" 
READ_LOCATION = "/tier2/freeman/streaming/streaminginput/" 
TEXT_WRITE_LOCATION = "/tier2/freeman/streaming/streamingoutput/"

analysis1 = Analysis.SeriesMeanAnalysis(input=READ_LOCATION, output=TEXT_WRITE_LOCATION, prefix="output", format="binary") 

tssc.set_checkpoint(CHECKPOINT_DIR)
tssc.set_checkpoint_interval("10000")
tssc.set_hadoop_block_size("1")
tssc.set_parallelism("100")
tssc.set_master("spark://h08u17.int.janelia.org:7077")
tssc.set_feeder_conf(NikitasFeederConf) 
tssc.set_batch_time("20")

tssc.add_analysis(analysis1)

NikitasFeederConf.set_linger_time(-1)

# For testing
NikitasFeederConf.set_max_files(10)
NikitasFeederConf.set_poll_time(5) 

updater = ExampleUpdater(tssc, analysis1.identifier) 
updater.start()
tssc._start_streaming_child()
