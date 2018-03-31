import logging

import sys

import fiona
import pandas as pd

logger = logging.getLogger(__name__)

EVERY = 1000


def get_sink(source, extra_props, sink_path):
    sink_schema = source.schema.copy()
    sink_schema['properties'] = dict(**source.schema['properties'],
                                     **extra_props)
    if 'Shape_area' in sink_schema['properties']:
        sink_schema['properties']['Shape_area'] = 'float:40.30'
    return fiona.open(
        sink_path, 'w',
        crs=source.crs,
        driver=source.driver,
        schema=sink_schema,
    )


def supplement_shapefile(shp_path, dat_path, column_map, join_key, sink_path):
    d_read = (
        pd
        .read_csv(dat_path, encoding='latin-1', skiprows=[1], chunksize=1000000)
    )
    sub_chunks = []
    for i, chunk in enumerate(d_read):
        chunk_sub = (
            chunk
            .loc[lambda d: d['STATEA'] == 1]
        )
        sub_chunks.append(chunk_sub)
        logger.info(f'Added chunk {i} with {len(chunk_sub)} rows')

        # Cheat because I know how the rows are laid out.
        if len(chunk_sub) == 0:
            break

    d = (
        pd
        .concat(sub_chunks)
        .set_index(join_key, verify_integrity=False)
        .filter(column_map.keys())
        .rename(columns={k: v[0] for k, v in column_map.items()})
    )

    extra_props = {v[0]: v[1] for v in column_map.values()}

    with fiona.open(shp_path) as source:
        nr_features = len(source)
        with get_sink(source, extra_props, sink_path) as sink:
            for i, feature in enumerate(source):
                props = feature['properties']
                try:
                    row = d.loc[props[join_key]]
                except KeyError:
                    logger.error(props[join_key])
                    continue
                else:
                    for col, val in row.iteritems():
                        prop_type = fiona.prop_type(extra_props[col])
                        props[col] = prop_type(val)
                if i % EVERY == 0:
                    logger.info(
                        f'{i} of {nr_features} ({100 * i / nr_features}%)'
                    )
                sink.write(feature)


if __name__ == '__main__':
    SHP_PATH = sys.argv[1]
    DAT_PATH = sys.argv[2]
    COL_MAP_PATH = sys.argv[3]
    SHP_SUPP_PATH = sys.argv[4]
    JOIN_KEY = 'GISJOIN'

    with open(COL_MAP_PATH) as file:
        rows = [
            row.strip().split(',') for row in file
            if row.strip() and not row.startswith('#')
        ]
    COLUMN_NAME_MAP = {row[0]: (row[1], row[2]) for row in rows}

    logging.basicConfig(level=logging.INFO)
    supplement_shapefile(SHP_PATH, DAT_PATH, COLUMN_NAME_MAP, JOIN_KEY,
                         SHP_SUPP_PATH)
