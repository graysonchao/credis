#!/usr/bin/env python

import argparse
import requests
import uuid
import time
import redis

import numpy as np

class SeqPutter:

    def __init__(self, args):
        self.args = args
        # Used as the channel name receiving acks.
        self.client_id = str(uuid.uuid4())
        self.ops_completed = 0

    def get_head(self):
        r = requests.get("http://{0}:{1}/head".format(self.args.master_host, self.args.master_port))
        response = r.json()
        return redis.StrictRedis(response["address"], int(response["port"]))

    def get_tail(self):
        r = requests.get("http://{0}:{1}/tail".format(self.args.master_host, self.args.master_port))
        response = r.json()
        client = redis.StrictRedis(response["address"], int(response["port"]))
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(self.client_id)
        return client, pubsub

    def SeqPut(self):
        """For i in range(n), sequentially put i->i into redis."""
        ack_client, ack_pubsub = self.get_tail()
        self.ack_pubsub = ack_pubsub
	self.head_client = self.get_head()
        self.ops_completed = 0

        latencies = []
        for i in range(self.args.num_writes):
            # if i % 50 == 0:
            # print('i = %d' % i)
            start = time.time()
            self.Put(i)  # i -> i
            latencies.append((time.time() - start) * 1e6)  # Microsecs.
            time.sleep(self.args.sleep_secs)
            self.ops_completed += 1  # No lock needed.

        nums = np.asarray(latencies)
        print(
            'throughput %.1f writes/sec; latency (us): mean %.5f std %.5f num %d' %
            (len(nums) * 1.0 / np.sum(nums) * 1e6, np.mean(nums), np.std(nums),
             len(nums)))

	assert(self.ops_completed == self.args.num_writes)
	self.Check()

    # Asserts that the redis state is exactly {i -> i | i in [0, n)}.
    def Check(self):
        read_client, _ = self.get_tail()
        actual = len(read_client.keys(b'*'))
        if actual != self.args.num_writes:
            print('head # keys: %d' % len(self.head_client.keys(b'*')))
	assert actual == self.args.num_writes, \
		       "Written %d Expected %d" % (actual, self.args.num_writes)
        for i in range(self.args.num_writes):
            data = read_client.get(str(i))
            assert int(data) == i, i


    # From redis-py/.../test_pubsub.py.
    def wait_for_message(self, timeout=0.1, ignore_subscribe_messages=False):
        now = time.time()
        timeout = now + timeout
        while now < timeout:
            message = self.ack_pubsub.get_message(
                ignore_subscribe_messages=ignore_subscribe_messages)
            if message is not None:
                return message
            time.sleep(1e-5)  # 10us.
            now = time.time()
        return None

    def Put(self, i):
        i_str = str(i)  # Serialize it once.
        put_issued = False

        # Try to issue the put.
        for k in range(3):
            try:
                sn = self.head_client.execute_command("MEMBER.PUT", i_str, i_str,
                                                 self.client_id)
                put_issued = True
                break
            except redis.exceptions.ConnectionError:
                self.head_client = RefreshHeadFromMaster(master_client)  # Blocking.
                continue
        if not put_issued:
            raise Exception("Irrecoverable redis connection issue; put client %s" %
                            head_client)

        # Wait for the ack.
        ack = None
        ack_client_okay = False
        for k in range(3):  # Try 3 times.
            try:
                # if k > 0:
                # print('k %d pubsub %s' % (k, ack_pubsub.connection))
                # NOTE(zongheng): 1e-4 seems insufficient for an ACK to be
                # delivered back.  1e-3 has the issue of triggering a retry, but
                # then receives an ACK for the old sn (an issue clients will need
                # to address).  Using 10ms for now.
                ack = self.wait_for_message(timeout=1e-2)
                ack_client_okay = True
                break
            except redis.exceptions.ConnectionError as e:
                _, self.ack_pubsub = self.get_tail()
                continue

        if not ack_client_okay:
            raise Exception("Irrecoverable redis connection issue; ack client %s" %
                            self.ack_pubsub.connection)
        elif ack is None:
            # Connected but an ACK was not received after timeout (the update was
            # likely ignored by the store).  Retry.
            self.fails_since_last_success += 1
            if self.fails_since_last_success >= max_fails:
                raise Exception(
                    "A maximum of %d update attempts have failed; "
                    "no acks from the store are received. i = %d, client = %s" %
                    (max_fails, i, self.ack_pubsub.connection))
            _, self.ack_pubsub = RefreshTailFromMaster(master_client, self.client_id)
            print("%d updates have been ignored since last success, "
                  "retrying Put(%d) with fresh ack client %s" %
                  (self.fails_since_last_success, i, self.ack_pubsub.connection))
            Put(i)
        else:
            # TODO(zongheng): this is a stringent check.  See NOTE above: sometimes
            # we can receive an old ACK.
            assert int(ack["data"]) == sn
            self.fails_since_last_success = 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-writes', '-n', default=1000, type=int,
                        help='Number of puts to perform (default 1000)')
    parser.add_argument('--sleep-secs', '-s', default=0.01, type=float,
                        help='Time to sleep between puts (default 0.01)')
    parser.add_argument('--master-host', '-m', default='credis_master_1',
                        help='Hostname of the master node (default credis_master_1)')
    parser.add_argument('--master-port', '-p', default=8080,
                        help='Listen port of the master node (default 8080)')
    args = parser.parse_args()
    putter = SeqPutter(args)
    putter.SeqPut()

if __name__ == '__main__':
    main()
