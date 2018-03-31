import sys
import logging
import csv
from itertools import chain, repeat, islice
import io

import pandas as pd
import numpy as np
from shapely import speedups
from shapely.geos import lgeos
from shapely.impl import DefaultImplementation
from shapely.prepared import prep
import shapely.geometry as sg
import fiona

import util

logger = logging.getLogger(__name__)


if speedups.available:
    speedups.enable()


contains = DefaultImplementation['prepared_contains'].fn


def pick_points_in_feature(n, feature):
    if n == 0:
        return []

    geom = sg.shape(feature['geometry'])
    bound_w, bound_s, bound_e, bound_n = geom.bounds
    prepared_geom = prep(geom)

    # 1 is the number of points; 2 is the number of spatial dimensions.
    coord_seq = lgeos.GEOSCoordSeq_create(1, 2)
    point = lgeos.GEOSGeom_createPoint(coord_seq)

    nr_pts_to_pick = n
    while nr_pts_to_pick:
        xs_to_try = np.random.uniform(bound_w, bound_e, size=nr_pts_to_pick)
        ys_to_try = np.random.uniform(bound_s, bound_n, size=nr_pts_to_pick)
        for x, y in zip(xs_to_try, ys_to_try):
            # Because of a bug in the GEOS C API, always set X before Y.
            # 0 is the index of the point to modify.
            lgeos.GEOSCoordSeq_setX(coord_seq, 0, x)
            lgeos.GEOSCoordSeq_setY(coord_seq, 0, y)
            if contains(prepared_geom._geom, point):
                yield x, y
                nr_pts_to_pick -= 1


POP_KEYS = (
    'no_hsp_wh',
    'no_hsp_bl',
    'no_hsp_nat',
    'no_hsp_as',
    'no_hsp_pa',
    'no_hsp_oth',
    'no_hsp_mlt',
    'hsp_wh',
    'hsp_bl',
    'hsp_nat',
    'hsp_as',
    'hsp_pa',
    'hsp_oth',
    'hsp_mlt',
)


def pick_labelled_points_in_feature(feature):
    props = feature['properties']
    nr_points = sum(props[k] for k in POP_KEYS)
    # Generate points within the geometry.
    points = pick_points_in_feature(nr_points, feature)
    labels = chain.from_iterable((repeat(k, props[k]) for k in POP_KEYS))
    yield from zip(points, labels)


def pick_labelled_points_in_shapefile(shapefile):
    return map(pick_labelled_points_in_feature, shapefile)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    shp_path = sys.argv[1]
    out_path = sys.argv[2]

    shapefile = fiona.open(shp_path)

    logger.info(f'Got {len(shapefile)} features')

    feature_sets = pick_labelled_points_in_shapefile(shapefile)
    labelled_pts = chain.from_iterable(feature_sets)
    flat_labelled_pts = map(lambda row: row[0] + (row[1],), labelled_pts)

    logger.info(f'Writing points...')
    util.stream_to_hdf(flat_labelled_pts, path=out_path, key='dots')
    logger.info(f'Wrote points.')
