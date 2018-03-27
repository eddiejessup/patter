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
    sink_schema['properties']['Shape_area'] = 'float:40.30'
    return fiona.open(
        sink_path, 'w',
        crs=source.crs,
        driver=source.driver,
        schema=sink_schema,
    )


def to_py_type(val):
    if type(val) == int:
        return int(val)
    elif type(val) == float:
        return int(val)
    else:
        return str(val)


def supplement_shapefile(shp_path, dat_path, column_map, join_key, sink_path):
    d = (
        pd
        .read_csv(dat_path, encoding='latin-1', skiprows=[1])
        .set_index(join_key, verify_integrity=True)
    )
    d_sub = (
        d
        .filter(column_map.keys())
        .rename(columns={k: v[0] for k, v in column_map.items()})
    )

    extra_props = {v[0]: v[1] for v in column_map.values()}

    with fiona.open(shp_path) as source:
        nr_features = len(source)
        # import pdb; pdb.set_trace()
        with get_sink(source, extra_props, sink_path) as sink:
            for i, feature in enumerate(source):
                props = feature['properties']
                try:
                    row = d_sub.loc[props[join_key]]
                except IndexError:
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
    # SHP_PATH = 'nhgis0003_shapefile_tl2010_us_tract_2010/US_tract_2010.shp'
    # SHP_SUPP_PATH = 'nhgis0003_shapefile_tl2010_us_tract_2010/US_tract_2010_supplement.shp'
    SHP_SUPP_PATH = 'supp.shp'
    # DAT_PATH = 'nhgis0005_ds172_2010_tract.csv'
    DAT_PATH = sys.argv[2]
    JOIN_KEY = 'GISJOIN'

    # COLUMN_NAME_MAP_TRACT = {
    #     'YEAR': ('year', 'int'),
    #     'REGIONA': ('region_A', 'int'),
    #     'DIVISIONA': ('division_A', 'int'),
    #     'STATE': ('state', 'str'),
    #     'STATEA': ('state_A', 'int'),
    #     'COUNTY': ('county', 'str'),
    #     'COUNTYA': ('county_A', 'int'),
    #     'COUSUBA': ('cnty_sub_A', 'int'),
    #     'PLACEA': ('place_A', 'int'),
    #     'TRACTA': ('tract_A', 'int'),
    #     'NAME': ('name', 'str'),
    #     'H7X001': ('pop_total', 'int'),
    #     'H7X002': ('pop_wh', 'int'),
    #     'H7X003': ('pop_bl', 'int'),
    #     'H7X004': ('pop_nat', 'int'),
    #     'H7X005': ('pop_as', 'int'),
    #     'H7X006': ('pop_pac', 'int'),
    #     'H7X007': ('pop_oth', 'int'),
    #     'H7X008': ('pop_mlt', 'int'),
    # }

    COLUMN_NAME_MAP = {
        'YEAR': ('year', 'int'),
        'REGIONA': ('region_A', 'int'),
        'DIVISIONA': ('division_A', 'int'),
        'STATE': ('state', 'str'),
        'STATEA': ('state_A', 'int'),
        'COUNTY': ('county', 'str'),
        'COUNTYA': ('county_A', 'int'),
        'COUSUBA': ('cnty_sub_A', 'int'),
        'PLACEA': ('place_A', 'int'),
        'TRACTA': ('tract_A', 'int'),
        'BLKGRPA': ('blk_grp_A', 'int'),
        'BLOCKA': ('blk_A', 'int'),

        'H7Z001': ('total', 'int'),
        'H7Z002': ('no_hsp', 'int'),
        'H7Z003': ('no_hsp_wh', 'int'),
        'H7Z004': ('no_hsp_bl', 'int'),
        'H7Z005': ('no_hsp_nat', 'int'),
        'H7Z006': ('no_hsp_as', 'int'),
        'H7Z007': ('no_hsp_pac', 'int'),
        'H7Z008': ('no_hsp_oth', 'int'),
        'H7Z009': ('no_hsp_mlt', 'int'),
        'H7Z010': ('hsp', 'int'),
        'H7Z011': ('hsp_wh', 'int'),
        'H7Z012': ('hsp_bl', 'int'),
        'H7Z013': ('hsp_nat', 'int'),
        'H7Z014': ('hsp_as', 'int'),
        'H7Z015': ('hsp_pac', 'int'),
        'H7Z016': ('hsp_oth', 'int'),
        'H7Z017': ('hsp_mlt', 'int'),
    }

    logging.basicConfig(level=logging.INFO)
    supplement_shapefile(SHP_PATH, DAT_PATH, COLUMN_NAME_MAP, JOIN_KEY,
                         SHP_SUPP_PATH)
