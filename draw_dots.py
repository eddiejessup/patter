import sys
import logging

import datashader as ds
from datashader.transfer_functions import shade
from datashader.utils import export_image
import dask.dataframe as dd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BACKGROUND = 'black'

COLOR_KEY = {
    'no_hsp_wh': BACKGROUND,
    # 'no_hsp_wh': 'red',
    'no_hsp_bl': BACKGROUND,
    # 'no_hsp_bl': 'aqua',
    'no_hsp_nat': 'orange',
    'no_hsp_as': 'pink',
    'no_hsp_pa': 'blue',
    'no_hsp_oth': 'blue',
    'no_hsp_mlt': 'blue',
    'hsp_wh': 'lime',
    'hsp_bl': 'red',
    'hsp_nat': 'aqua',
    'hsp_as': 'blue',
    'hsp_pa': 'blue',
    'hsp_oth': 'blue',
    'hsp_mlt': 'blue',
}

PLOT_WIDTH  = 2000

in_path = sys.argv[1]
in_key = sys.argv[2]
out_path = sys.argv[3]

logger.info('Reading data...')
df = dd.read_hdf(in_path, in_key, start=0)
df.columns = ['easting', 'northing', 'race']
df = (
    df
    .categorize(columns=['race'])
    .persist()
)
logger.info('Read data.')

logger.info('Creating image...')
x_range = (df.easting.min().compute(), df.easting.max().compute())
y_range = (df.northing.min().compute(), df.northing.max().compute())

plot_ratio = (y_range[1] - y_range[0]) / (x_range[1] - x_range[0])
plot_height = int(PLOT_WIDTH * plot_ratio)

canvas = ds.Canvas(
    plot_width=PLOT_WIDTH, plot_height=plot_height,
    x_range=x_range, y_range=y_range,
)
agg = canvas.points(df, 'easting', 'northing', ds.count_cat('race'))
image = shade(agg, color_key=COLOR_KEY, how='eq_hist')
logger.info('Created image.')

logger.info('Saving image...')
export_image(image, out_path, fmt='.png', background=BACKGROUND)
logger.info('Saved image.')
