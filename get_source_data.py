import os
import sys
import zipfile
import subprocess

DATA_DIR = '../data'
ZIP_DIR = os.path.join(DATA_DIR, 'zips')
ZIP_FILE_TMPL = 'tabblock2010_{}_pophu.zip'
URL_PREFIX = 'ftp://ftp2.census.gov/geo/tiger/TIGER2010BLKPOPHU'
EXTRACT_DIR = os.path.join(DATA_DIR, 'extracted')


if __name__ == '__main__':
    with open(sys.argv[1]) as states_file:
        states = dict(
            ln.strip().split(',')
            for ln in states_file
            if ln and not ln.startswith('#')
        )

    download_list_str = ' '.join(
        os.path.join(URL_PREFIX, ZIP_FILE_TMPL.format(state_idx))
        for state_idx in states.values()
    )
    # --continue: Continue getting a partially-downloaded file.
    subprocess.run(['wget', '--continue',
                   '--directory-prefix', ZIP_DIR, download_list_str])

    for state, state_idx in states.items():
        print('Unzipping for {}...'.format(state))
        zip_path = os.path.join(ZIP_DIR, ZIP_FILE_TMPL.format(state_idx))
        with zipfile.ZipFile(zip_path) as zip_file:
            zip_file.extractall(EXTRACT_DIR)
