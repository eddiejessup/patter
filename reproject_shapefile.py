import sys
import logging

import fiona
from fiona.crs import from_epsg
import geopandas as gpd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TO_EPSG = 4326

in_path = sys.argv[1]
out_path = sys.argv[2]

logger.info('Reading file...')
gshp = gpd.read_file(in_path)
logger.info('Read file.')


logger.info('Reprojecting...')
gshp.to_crs(epsg=TO_EPSG, inplace=True)
gshp.crs = from_epsg(TO_EPSG)
logger.info('Reprojected.')

logger.info('Saving...')
gshp.to_file(out_path)
logger.info('Saved.')
