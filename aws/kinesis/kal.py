from __future__ import print_function
import boto
from pprint import pprint

class KinesisStream(object):
    def __init__(self, stream_name, dry_run=False, verbose=False):
        self.stream_name = stream_name
        self.dry_run = dry_run
        self.verbose = verbose
        self.kinesis = boto.connect_kinesis()
        self.stream_status = None
        self.open_shards = []

        response = self.kinesis.describe_stream(stream_name)
        if self.verbose:
            pprint(response)
        self.stream_status = response['StreamDescription']['StreamStatus']
        self.open_shards += [s for s in response['StreamDescription']['Shards'] if 'EndingSequenceNumber' not in s['SequenceNumberRange']]
        while response['StreamDescription']['HasMoreShards']:
            response = self.kinesis.describe_stream(stream_name, exclusive_start_shard_id=response['StreamDescription']['Shards'][-1]['ShardId'])
            if self.verbose:
                pprint(response)
            self.open_shards += [s for s in response['StreamDescription']['Shards'] if 'EndingSequenceNumber' not in s['SequenceNumberRange']]
        self.open_shards = sorted(self.open_shards, key=lambda s: long(s['HashKeyRange']['EndingHashKey']))
        if self.verbose:
            pprint(self.open_shards)

    def display(self):
        print('Stream Status: {0}'.format(self.stream_status))
        print('')
        print('{0:^20s} | {1:^32s} | {2:^32s} | {3:^32s}'.format('Shard Id', 'Starting Hash Key', 'Ending Hash Key', 'Span'))
        print('{0}-+-{1}-+-{2}-+-{3}'.format('-'*20, '-'*32, '-'*32, '-'*32))
        for s in self.open_shards:
            hash_key_range = s['HashKeyRange']
            starting_hash_key = long(hash_key_range['StartingHashKey'])
            ending_hash_key = long(hash_key_range['EndingHashKey'])
            print('{shard_id} | {starting_hash_key:032X} | {ending_hash_key:032X} | {range:032X}'.format(shard_id=s['ShardId'], starting_hash_key=starting_hash_key, ending_hash_key=ending_hash_key, range=1+ending_hash_key-starting_hash_key))

    def split_shard(self, shard_to_split):
        for shard in self.open_shards:
            if shard['ShardId'] == shard_to_split:
                hash_key_range = shard['HashKeyRange']
                range_sum = long(hash_key_range['EndingHashKey']) + long(hash_key_range['StartingHashKey'])
                if range_sum % 2 == 1:
                    range_sum += 1
                new_starting_hash_key = str(range_sum/2)
                if self.verbose:
                    print('Splitting {shard_to_split} at {new_starting_hash_key}'.format(shard_to_split=shard_to_split, new_starting_hash_key=new_starting_hash_key))
                if not self.dry_run:
                    self.kinesis.split_shard(self.stream_name, shard_to_split, new_starting_hash_key)
                break

    def merge_shards(self, shard_to_merge, adjacent_shard_to_merge):
        if self.verbose:
            print('Merging {shard_to_merge} and {adjacent_shard_to_merge}'.format(shard_to_merge=shard_to_merge, adjacent_shard_to_merge=adjacent_shard_to_merge))
        if not self.dry_run:
            self.kinesis.merge_shards(self.stream_name, shard_to_merge, adjacent_shard_to_merge)
