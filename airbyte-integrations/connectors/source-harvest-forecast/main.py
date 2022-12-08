#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_harvest_forecast import SourceHarvestForecast

if __name__ == "__main__":
    source = SourceHarvestForecast()
    launch(source, sys.argv[1:])
