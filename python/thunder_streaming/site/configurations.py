from thunder_streaming.shell.feeder_configuration import FeederConfiguration
"""
Example configurations
"""

# Nick's
NicksFeederConf = FeederConfiguration()
NicksFeederConf.set_images_dir("/groups/freeman/freemanlab/Streaming/demo_2015_01_16/registered_im")
NicksFeederConf.set_behaviors_dir("/groups/freeman/freemanlab/Streaming/demo_2015_01_16/registered_bv")
NicksFeederConf.set_spark_input_dir("/nobackup/freeman/streaminginput/")
NicksFeederConf.set_max_files(40)
NicksFeederConf.set_linger_time(300.0)
NicksFeederConf.set_poll_time(15.0)
NicksFeederConf.set_image_prefix("images")
NicksFeederConf.set_behaviors_prefix("behaviours")

# Nikita's
NikitasFeederConf = FeederConfiguration()
NikitasFeederConf.set_images_dir("/groups/ahrens/ahrenslab/Nikita/Realtime/imaging/test1_*")
NikitasFeederConf.set_behaviors_dir("/groups/ahrens/ahrenslab/Nikita/Realtime/ephys/")
NikitasFeederConf.set_spark_input_dir("/nobackup/freeman/streaminginput/")
NikitasFeederConf.set_timepoint_regexes(FeederConfiguration.RegexList(["TM(\d+)[_\.].*"]))
NikitasFeederConf.set_prefix_regexes(FeederConfiguration.RegexList(["behav   TM\d+\.10ch", "img TM\d+_.*\.stack"]))
NikitasFeederConf.set_mod_buffer_time(5)
NikitasFeederConf.set_max_files(-1)
NikitasFeederConf.set_check_size()

# Testing on cluster (Feeder output and Spark input set at the same time elsewhere)
ClusterTestingFeederConf = FeederConfiguration()
ClusterTestingFeederConf.set_images_dir("/groups/freeman/freemanlab/Streaming/demo_2015_02_20b/registered_im")
ClusterTestingFeederConf.set_behaviors_dir("/groups/freeman/freemanlab/Streaming/demo_2015_02_20b/registered_bv")
ClusterTestingFeederConf.set_max_files(40)
ClusterTestingFeederConf.set_image_prefix("images")
ClusterTestingFeederConf.set_behaviors_prefix("behaviour")
ClusterTestingFeederConf.set_poll_time(15.0)
ClusterTestingFeederConf.set_linger_time(300.0)
