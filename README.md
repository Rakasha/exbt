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
iex> myjob = Job.create("/tmp/test_job", "test/centos.torrent")
...
...
iex> Job.start(myjob)
```

Or from existing job dir:
```elixir
iex> myjob = Job.load("/tmp/test_job")
...
...
iex> Job.start(myjob)
```

### Avaiable Commands

#### Create a new job 
```elixir
myjob = Job.create(<job_dir>, <path_of_torrent_file>)
```

#### Load existing job
```elixir
myjob = Job.load(<job_dir>)
```

#### Start the job
Start connecting to peers and start downloading
```elixir
Job.start(myjob)
```

#### Stop the job
Disconnect from peers. Stop all actions.
```elixir
Job.stop(myjob)
```

#### Pause download/uploac
Stop download/uploading data, but keep current connections
```elixir
Job.pause(myjob)
```

#### Resume download/upload
Start download/uploading data
```elixir
Job.resume(myjob)
```

#### Getting progress of download
```elixir
iex(5)> Job.progress(job)
%{finished: 1105, percentage: 100.0, unfinished: 0}
```
OR
```elixir
iex(6)> Job.finished?(myjob)
true
```

#### Set the max-allowed connections
```elixir
Job.set_max_connections(myjob, 5)
```

#### Display file info listed in the torrent metadata
```elixir
iex(8)> Job.files(myjob)
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
iex(9)> Job.status(myjob)
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
iex(10)> Job.dir(myjob)
"/tmp/test_bt"
```

References
========

https://www.bittorrent.org/beps/bep_0003.html

http://www.bittorrent.org/beps/bep_0005.html

http://www.bittorrent.org/beps/bep_0009.html

https://www.libtorrent.org/dht_store.html


Documents
========


## DHTBucket

### DHTBucket.Struct

- capacity
- nodes
- candidates
- last_changed

Bucket `<id>` contains node with distance falls between
`[2^<id-1>, 2^<id> - 1]`

For example a DHT table with 4-bit node-space should have buckets:
```
bucket0: 0000 ~ 0000 (current node)
bucket1: 0001 ~ 0001
bucket2: 0010 ~ 0011
bucket3: 0100 ~ 0111
```

### DHTBucket.Functions
- new_bucket
- add_node
- add_candidate
- delete_node
- get_node_by_id
- get_nodes
- get_n_nodes
- has_node?
- has_candidate?
- without_node(node_id)
- get_most_trusty_node
- get_trusty_nodes
- get_most_questionable_node
- get_questionable_nodes
- size
- size_info
- is_full?
- is_fresh?
- refresh_node
- insert_to_sorted_nodes
- healthcheck_threshold

--------------------------------

## DHTTable

### DHTTable.Attributes
- buckets
- reference_node_id


### DHTTable.Functions
- new_table
- get_node
- reference_node_id
- set_bucket
- get_k_nearest_nodes(distance)
- get_bucket_num_by_distance(distance)
- get_corresponding_bucket
- get_corresponding_bucket_num
- size

--------------------------------

## KRPC_Msg

### KRPC_Msg.Functions

General functions:
- get_error_types
- get_sender_id
- error_code
- to_bin
- from_bin
- from_buffer
- gen_transaction_id
- get_query_type

Functions that build KRPC messages:
- ping
- ping_response
- find_node
- find_node_response
- get_peers
- get_peers_response
- announce_peer
- announce_peer_response
- response
- error

--------------------------------

## DHTServer

- @ping_response_threshold
- @ping_response_threshold_sec
- @krpc_query_timeout
- @api_general_timeout
- @api_search_nodes_timeout

### DHTServer.Struct (as GenServer state)
  - node_id: node_id
  - table: DHTTable
  - peer_info: %{info_hash => compact_peer_list}
  - token_server: token server pid
  - waiting_response: %{transaction_id => {req_msg, req_pid} }
  - socket: udp socket

### DHTServer.Functions
- start_link
- init
- ping
- find_node
- node_id
- search_peers
- search_nodes
- schedule_node_check
- send_krpc_msg!
- do_add_node
- do_on_krpc_msg

### DHTServer.handle_call

- :node_id
- :ping
- :add_node
- :find_node
- :get_peers
- :stored_nearest_nodes
- :send_krpc_msg

### DHTServer.handle_cast
- :bootstrap

### DHTServer.handle_info
- :udp
- :krpc
- :check_node_aliveness
- :krpc_query_timeout

--------------------------------

## TokenServer

### TokenServer.State:
  - secret
  - old_secret

### TokenServer.Functions

- start_link
- init
- is_valid_token?
- request_token
- random_secret
- calculate_token

### TokenServer.handle_call
- :is_valid_token/2
- :request_token/1

### TokenServer.handle_info
- :loop_renew_secret

--------------------------------

## DHTNode

### DHTNode.Struct:
  - id
  - ip
  - port
  - last_active

### DHTNode.Functions
- compact_info
- contact_info

--------------------------------

## DHTUtils

### DHTUtils.Functions

- count_0_prefix_length
- node_distance_binary
- gen_random_bitstring
- gen_random_string
- gen_node_id
- parse_peers_val
- parse_nodes_val
- get_magnet_info_hash

