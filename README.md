# exbt
This script is still in beta.

Naive implementation of BitTorrent Client (in Elixir), quick and dirty.

## Usage

### Initializing a job

Run `iex exbt.exs` to load the module into interactive shell

Create a new download job by providing:
 1. The .torrent file
 2. An **non-exist** dir for storing job's data

```elixir
iex> job = Job.create("/tmp/test_job", "test/centos.torrent")
...
...
iex> Job.start(job)
```

Or from existing job dir:
```elixir
iex> job = Job.load("/tmp/test_job")
...
...
iex> Job.start(job)
```

### Avaiable Commands

#### Create a new job 
```elixir
Job.create(<job_dir>, <torrent_path>)
```

#### Load existing job
```elixir
Job.load(<job_dir>)
```

#### Start the job
Start connecting to peers and start downloading
```elixir
Job.start(job)
```

#### Stop the job
Disconnect from peers. Stop all actions.
```elixir
Job.stop(job)
```

#### Pause download/uploac
Stop download/uploading data, but keep current connections
```elixir
Job.pause(job)
```

#### Resume download/upload
Start download/uploading data
```elixir
Job.resume(job)
```

#### Getting progress of download
```elixir
iex(5)> Job.progress(job)
%{finished: 1105, percentage: 100.0, unfinished: 0}
```
OR
```elixir
iex(6)> Job.finished?(job)
true
```

#### Set the max-allowed connections
```elixir
Job.set_max_connections(job, 5)
```

#### Display file info listed in the torrent metadata
```elixir
iex(8)> Job.files(job)
[
  %{
    "length" => 578813952,
    "offset_start" => 0,
    "path" => "CentOS-7-x86_64-NetInstall-1908/CentOS-7-x86_64-NetInstall-1908.iso",
    "piece_index_range" => 0..1104
  },
  %{
    "length" => 598,
    "offset_start" => 578813952,
    "path" => "CentOS-7-x86_64-NetInstall-1908/sha256sum.txt",
    "piece_index_range" => 1104..1104
  },
  %{
    "length" => 1458,
    "offset_start" => 578814550,
    "path" => "CentOS-7-x86_64-NetInstall-1908/sha256sum.txt.asc",
    "piece_index_range" => 1104..1104
  }
]
```

#### Display overall status
```elixir
iex(9)> Job.status(job)
%{
  connections: 0,
  files: [
    %{
      "length" => 578813952,
      "offset_start" => 0,
      "path" => "CentOS-7-x86_64-NetInstall-1908/CentOS-7-x86_64-NetInstall-1908.iso",
      "piece_index_range" => 0..1104
    },
    %{
      "length" => 598,
      "offset_start" => 578813952,
      "path" => "CentOS-7-x86_64-NetInstall-1908/sha256sum.txt",
      "piece_index_range" => 1104..1104
    },
    %{
      "length" => 1458,
      "offset_start" => 578814550,
      "path" => "CentOS-7-x86_64-NetInstall-1908/sha256sum.txt.asc",
      "piece_index_range" => 1104..1104
    }
  ],
  job_dir: "/tmp/test_bt",
  progress: %{finished: 1105, percentage: 100.0, unfinished: 0}
}
```

#### Get the job's dir

```elixir
iex(10)> Job.dir(job)
"/tmp/test_bt"
```
