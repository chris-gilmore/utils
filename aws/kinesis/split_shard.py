#!/usr/bin/env python

import argparse
import sys
import kal

def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('STREAM_NAME', help='kinesis stream name')
    parser.add_argument('-d', '--display', action='store_true', help='display open shards')
    parser.add_argument('-s', '--shard', metavar='SHARD_ID', help='shard to split')
    parser.add_argument('-n', '--dry-run', action='store_true', help='perform a trial run')
    parser.add_argument('-v', '--verbose', action='store_true', help='produce more output')
    args = parser.parse_args()

    if not (args.display or args.shard):
        parser.print_help()
        sys.exit()

    kinesis_stream = kal.KinesisStream(stream_name=args.STREAM_NAME, dry_run=args.dry_run, verbose=args.verbose)

    if args.display:
        kinesis_stream.display()

    if args.shard:
        kinesis_stream.split_shard(shard_to_split=args.shard)

if __name__ == "__main__":
    main()
