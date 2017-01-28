#!/usr/bin/env python2.7

from subprocess import call, check_output

def clean_string(string):
    return string.strip(' \t\n\r')

def get_snapshot_name(snapshot):
    return snapshot.split('@')[-1]

def get_dataset_name(dataset):
    return dataset.split('@')[0].split('/')[-1]

def build_full_snapshot(root_dataset, dataset, snapshot):
    return root_dataset + '/' + dataset + '@' + snapshot

ssh_target = 'some_server' # hostname of a server from the .ssh/config file
source_pool = 'tank/some/dataset' # source dataset on the server
target_pool = 'zpool/some/other/dataset' # local dataset to be used as target

source_datasets = map(clean_string, check_output([ 'ssh', ssh_target, 'zfs', 'list', '-H', '-r', '-o', 'name', source_pool, '|', 'tail', '-n', '+2' ]).splitlines())
if source_pool in source_datasets: source_datasets.remove(source_pool)

target_datasets = map(clean_string, check_output([ '/sbin/zfs', 'list', '-H', '-r', '-o', 'name', target_pool ]).splitlines())
if target_pool in target_datasets: target_datasets.remove(target_pool)

for source_dataset in source_datasets:
    dataset_name = get_dataset_name(source_dataset)

    source_snapshots = map(clean_string, check_output([ 'ssh', ssh_target, 'zfs', 'list', '-H', '-r', '-t', 'snapshot', '-o', 'name', '-s', 'creation', source_dataset ]).splitlines())
    last_source_snapshot = source_snapshots[-1]
    snapshot_name = get_snapshot_name(last_source_snapshot)

    print ' '
    print 'source_dataset: ' + source_dataset
    print 'source_snapshots: ' + str(source_snapshots)
    print 'last_source_snapshot: ' + last_source_snapshot
    print 'snapshot_name: ' + snapshot_name
        
    target_dataset = target_pool + '/' + dataset_name
    target_snapshot = target_dataset + '@' + snapshot_name

    print 'target_dataset: ' + target_dataset
    # print 'target_snapshot: ' + target_snapshot

    # # if target doesn't has the dataset, do a total zfs send/receive
    if target_dataset not in target_datasets:
        cmd = [ 'ssh', ssh_target, 'zfs', 'send', last_source_snapshot, '|', 'zfs', 'receive', target_snapshot ]
        print 'FULL cmd: ' + " ".join(cmd)
        print 'zfs send/receive ended with: ' + str(call(" ".join(cmd), shell=True))
        
    else:
        # if the target already has the dataset, check for the latest snapshot
        target_snapshots = map(clean_string, check_output([ 'zfs', 'list', '-H', '-r', '-t', 'snapshot', '-o', 'name', '-s', 'creation', target_dataset ]).splitlines())
        
        print 'target_snapshots: ' + str(target_snapshots)

        # don't do anything if is snapshot already present on the target side
        if target_snapshot not in target_snapshots:

            # get last snapshot of target snapshots
            last_target_snapshot = target_snapshots[-1]
            last_target_snapshot_name = get_snapshot_name(last_target_snapshot)

            # build source snapshot name
            source_snapshot = build_full_snapshot(source_pool, dataset_name, last_target_snapshot_name)

            # find snapshot in source snapshots
            if source_snapshot in source_snapshots:
                cmd = [ 'ssh', ssh_target, 'zfs', 'send', '-I', source_snapshot, last_source_snapshot, '|', 'zfs', 'receive', '-d', '-F', target_dataset ]
                print 'INCREMENTAL cmd: ' + " ".join(cmd)
                print 'zfs send/receive ended with: ' + str(call(" ".join(cmd), shell=True))
            else:
                print 'Must do full backup of dataset ' + dataset_name + '. Snapshot ' + last_target_snapshot_name + ' not found in source.'
                # rename target dataset
                # then do full

        else:
            print 'NOT NEEDED'
