# Parallel rsync to rapidly upload and download data from shared folders from the cluster

First, create an alias in your `~/.bashrc`:
```bash
alias prsync_c="/path/to/prsync_c"
```

Make sure to update your `.ssh/config` with all the cluster addresses like so
```bash
Host 0
    HostName 39.105.219.118
    User andrew.choi
    Port 50022
Host 1
    HostName 39.106.85.150
    User andrew.choi
    Port 50022
Host 2
    HostName 39.107.81.216
    User andrew.choi
    Port 50022
...
```
Then you download and upload data from **shared** folders from the cluster by leveraging the bandwidth of each individual server.

```bash
RSYNC_RSH="ssh -p 50022" prsync_c --parallel=8 --hosts="1" -avz andrew.choi@1:/data/datasets/openx/jaco_play/ jaco_play
```



## README from original author

This script is a simple drop-in replacement for `rsync` for parallelising your data transfer.

Rsync is the tool of choice for copying/syncing data between locations.
It is capable of only transfering files which have changed and resuming upload/downloads.
However, the transfer speed of a single `rsync` can be somewhat slow.
This is a problem when transfering a large amount of data as it will take some time to complete.

If your rsync contains lots of files, you can benefit from transfering files in parallel.
Thus benfiting from a more effective use of your available network bandwidth and gettging the job done faster.

# Usage

If your `rsync` command looks like this:

```bash
rsync \
  --times --recursive --progress \
  --exclude "raw_reads" --exclude ".snakemake" \
  user@example.com:/my_remote_dir/ /my_local_dir/
```

Simply replace the `rsync` executable for this script:

```bash
./prsync \
  --times --recursive --progress \
  --exclude "raw_reads" --exclude ".snakemake" \
  user@example.com:/my_remote_dir/ /my_local_dir/
```

## Number of Parallel Jobs

By default, the script will use 1 parallel job for each processor on the machine.
This is determined by `nproc` and if this fails, we fall back to `10` parallel jobs for transfering files.
This behaviour can be overriden by using `--parallel` as the first command line argument to the script:

```bash
./prsync \
  --parallel=20 \
  --times --recursive --progress \
  --exclude "raw_reads" --exclude ".snakemake" \
  user@example.com:/my_remote_dir/ /my_local_dir/
```

# Implementation

The list of files to be transfered is calulated by first running `rsync` in dry-run mode.
It is then split into `N` chunks based on the value of `--parallel` (10 by default).
Each "chunk" of files is then passed to parallel `rsync` process.

To ensure a more balanced distribution of files among chunks, files are sorted by decreasing filesize and then assigned to the chunk with the least data to process.
This ensures that chunks are of approximately the same size and have the same number of files to process.
Thus parallel `rsync` processes will complete at around the same time.
