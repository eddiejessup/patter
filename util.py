import logging
from io import StringIO
import csv
from itertools import islice

import pandas as pd


logger = logging.getLogger(__name__)

class CSVStream:

    NEWLINE_CHAR = '\n'

    def __init__(self, iterable, chunk_size):
        self.src = iterable
        self.chunk_size = chunk_size
        self.buffer = ''

    def extend_buffer(self):
        placeholder = StringIO()
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


def stream_to_hdf(rows, path, key, chunk_size=10000, complevel=9):
    csv_stream = CSVStream(rows, chunk_size=chunk_size)
    df = pd.read_csv(csv_stream, chunksize=chunk_size)
    for i, chunk in enumerate(df):
        logger.info(f'Writing chunk {i} with {len(chunk)} rows')
        chunk.to_hdf(path, key, append=True, format='table',
                     complevel=complevel)
