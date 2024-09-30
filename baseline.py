
BASELINE_VERSION = '1.0.0'


if __name__ == '__main__':
    import argparse
    import pathlib

    ap = argparse.ArgumentParser(
        description='Runs a simple baseline model that buys when a good stock is found, '
                    'and sells as soon as the first hill peak is found'
    )
    ap.add_argument(
        '-c', '--config',
        type=pathlib.Path, default=pathlib.Path('./config.json'),
        help='the location of the settings config file'
    )
    ap.add_argument(
        '--no_paper',
        action='store_true',
        help='indicates not to use paper markets'
    )
    ap.add_argument('-v', '--version', action='store_true', help='show the version and exit')
    args = ap.parse_args()

    if args.version:
        print('baseline model v{}'.format(BASELINE_VERSION))
        exit(0)
