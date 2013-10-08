#!/usr/bin/env python

import sys, boto
from time import sleep

def retry(f):
    retries = 6
    timeout = 1
    attempt = 0
    while attempt < retries:
        try:
            f()
            break
        except Exception as e:
            pass

        sleep(timeout)
        # exponential backoff
        timeout *= 2

        attempt += 1
    if attempt == retries:
        f()

def main():
    if len(sys.argv) < 2 or not sys.argv[1]:
        print >> sys.stderr, "empty or missing arguments"
        sys.exit(1)
    image_id = sys.argv[1]

    conn = boto.connect_ec2()
    img = conn.get_all_images(image_ids=[image_id])[0]
    snapshots = [device.snapshot_id for device in img.block_device_mapping.values() if device.snapshot_id]
    conn.deregister_image(img.id)
    for snap_id in snapshots:
        retry(lambda: conn.delete_snapshot(snap_id))

if __name__ == "__main__":
    main()
