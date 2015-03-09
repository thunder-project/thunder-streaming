"""
A few conversions specific to the StreamingSeries data type
"""

def seriesToImage(series_dir):
    """
    :param series: The fully-qualified directory name containing the chunked series output files
    :return: An in-memory representation of the series where keys have been removed and the values have been
        arranged so that they can be viewed as an image
    """
    pass

def seriesToPackedSeries(series_dir):
    """

    :param series_dir: The fully-qualified directory name containing the chunked series output files
    :return: An in-memory representation of the series where each chunk has been collected with indices included
    """
    pass