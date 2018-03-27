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

logger = logging.getLogger(__name__)


if speedups.available:
    speedups.enable()


def pick_points_in_feature(n, feature):
    if n == 0:
        return []

    geom = sg.shape(feature['geometry'])
    ll, bb, rr, tt = geom.bounds
    prepared_geom = prep(geom)

    contains = DefaultImplementation['prepared_contains'].fn

    # 2 is the dimension.
    coord_seq = lgeos.GEOSCoordSeq_create(1, 2)
    point = lgeos.GEOSGeom_createPoint(coord_seq)

    nr_pts_to_pick = n
    while nr_pts_to_pick:
        xs_to_try = np.random.uniform(ll, rr, size=nr_pts_to_pick)
        ys_to_try = np.random.uniform(bb, tt, size=nr_pts_to_pick)
        for x, y in zip(xs_to_try, ys_to_try):
            # Because of a bug in the GEOS C API, always set X before Y.
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
    'no_hsp_pac',
    'no_hsp_oth',
    'no_hsp_mlt',
    'hsp_wh',
    'hsp_bl',
    'hsp_nat',
    'hsp_as',
    'hsp_pac',
    'hsp_oth',
    'hsp_mlt',
)


def pick_pop_points_in_feature(feature):
    yield from pick_points_in_feature(feature['properties']['total'], feature)


def pick_labelled_points_in_feature(feature):
    props = feature['properties']
    # Generate a sample within the geometry for each person.
    points = pick_pop_points_in_feature(feature)
    labels = chain.from_iterable((repeat(k, props[k]) for k in POP_KEYS))
    yield from zip(points, labels)


def pick_labelled_points_in_shapefile(shapefile):
    return map(pick_labelled_points_in_feature, shapefile)
    # for j, feature in enumerate(shapefile):
    #     if j % 100 == 0:
    #         logger.info('{} features of {} ({:.1f}%)'
    #                     .format(j, nr_features, 100 * j / nr_features))


class CSVStream:

    NEWLINE_CHAR = '\n'

    def __init__(self, iterable, chunk_size):
        self.src = iterable
        self.chunk_size = chunk_size
        self.buffer = ''

    def extend_buffer(self):
        placeholder = io.StringIO()
        writer = csv.writer(placeholder, lineterminator=self.NEWLINE_CHAR)
        writer.writerows(islice(self.src, self.chunk_size))
        chunk_str = placeholder.getvalue()
        if chunk_str:
            self.buffer += chunk_str
        else:
            raise StopIteration

    def get_buffer_remainder(self):
        result = self.buffer
        self.buffer = ''
        return result

    def __iter__(self):
        return self

    def __next__(self):
        while self.NEWLINE_CHAR not in self.buffer:
            try:
                self.extend_buffer()
            except StopIteration:
                if self.buffer:
                    return self.get_buffer_remainder()
                else:
                    raise
        i_newline = self.buffer.index(self.NEWLINE_CHAR)
        result = self.buffer[:i_newline]
        # Note: Newline character itself is discarded.
        self.buffer = self.buffer[i_newline + 1:]
        return result

    def _read(self, n):
        result = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return result

    def read(self, n=-1):
        while n == -1 or len(self.buffer) < n:
            try:
                self.extend_buffer()
            except StopIteration:
                return self.get_buffer_remainder()
        return self._read(n)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    shp_path = sys.argv[1]
    out_path = sys.argv[2]

    shapefile = fiona.open(shp_path)
    feature_sets = pick_labelled_points_in_shapefile(shapefile)
    labelled_pts = chain.from_iterable(feature_sets)
    flat_labelled_pts = map(lambda row: row[0] + (row[1],), labelled_pts)

    # proj_this = pyproj.Proj(**shapefile.crs)
    # proj_webm = pyproj.Proj(init='epsg:3857')
    # transform = functools.partial(pyproj.transform, proj_this, proj_webm)
    # def proj_row(row):
    #     x, y, label = row
    #     xp, yp = transform(x, y)
    #     return xp, yp, label
    # rows = map(proj_row, rows)

    # flat_labelled_pts = islice(flat_labelled_pts, 3200)
    csv_stream = CSVStream(flat_labelled_pts, chunk_size=10000)
    df = pd.read_csv(csv_stream, chunksize=10000)
    for i, chunk in enumerate(df):
        print(i)
        # chunk.to_msgpack('out_2.msg', append=True)
        chunk.to_hdf('out_2.h5', 'dots', append=True, format='table',
                     complevel=9)
    # with open(out_path, 'w') as out_file:
        # writer = csv.writer(out_file)
        # writer.writerows(islice(flat_labelled_pts, 800000))
        # writer.writerows(flat_labelled_pts)
