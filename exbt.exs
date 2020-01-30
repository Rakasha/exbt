defmodule FileServer do
  require Logger
  use GenServer

  defstruct [
    event_pid: nil,
    download_dir: nil,
    torrent_file: nil,
    progress_file: nil,
    trackers_file: nil,
    decoded_torrent: nil,
    torrent_data: nil,
    piece_cache: nil,
    metadata_piece_cache: nil,
  ]

  def test_all() do
    test_find_gaps()
    test_intersect()
    test_piece_locations()
    info_hash = FileServer.calculate_torrent_info_hash("test/ubuntu.torrent")
    "B7B0FBAB74A85D4AC170662C645982A862826455" = Base.encode16(info_hash)
    FileServer.calculate_torrent_info_hash("test/centos.torrent")
  end

  def start(init_args) do
    GenServer.start(__MODULE__, init_args)
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  def init(init_args) do
    Logger.info("init_args: #{inspect(init_args)}")

    case init_args do
      {download_dir, torrent_or_magnet} ->
        if File.exists?(torrent_or_magnet) do
          init_from_new_torrent(download_dir, torrent_or_magnet)
        else
          {:ok, info_hash} = DHTUtils.get_magnet_info_hash(torrent_or_magnet)
          init_from_torrent_info_hash(download_dir, info_hash)
        end
      download_dir ->
        init_from_existing_job(download_dir)
    end
  end

  def init_from_existing_job(download_dir) do
    # Load from half-downloaded job's dir
    File.cd!(download_dir)
    torrent_file = Path.join(get_job_status_dir(), "job.torrent")

    if not File.exists?(torrent_file) do
      info_hash_file = Path.join(download_dir, "info_hash.bin")
      info_hash = File.read!(info_hash_file)
      torrent_data = %{info_hash: info_hash}
      init_state = %FileServer{
        download_dir: download_dir,
        torrent_data: torrent_data,
      }
      Logger.info("FileServer initialized: #{inspect(init_state)}")
      {:ok, init_state}
    else
      progress_file = Path.join(get_job_status_dir(), "progress.txt")
      trackers_file = Path.join(get_job_status_dir(), "trackers.txt")
      decoded_torrent = parse_torrent_file(torrent_file)
      piece_hash_list = Enum.chunk_every(:binary.bin_to_list(decoded_torrent["info"]["pieces"]), 20)
      file_info_list = collect_file_info(decoded_torrent)

      # Map files described in torrent metadata to existing filesystem
      # Also calculate the corresponding piece-index of each file
      file_info_list2 =
        Enum.map(
          file_info_list,
          fn file_info ->
            case target_file_progress(file_info, progress_file) do
              {_, 0} ->
                file_info

              _else ->
                Map.replace!(file_info, "path", file_info["path"] <> ".downloading")
            end
          end
        )

      piece_length = decoded_torrent["info"]["piece length"]
      total_length = List.foldl(file_info_list2, 0, fn x, total -> total + x["length"] end)
      piece_counts = :erlang.ceil(total_length / piece_length)

      last_piece_length =
        case Integer.mod(total_length, piece_length) do
          0 -> piece_length
          r -> r
        end

      torrent_data = %{
        tracker_url: decoded_torrent["announce"],
        info_hash: calculate_torrent_info_hash(torrent_file),
        files: file_info_list2,
        total_length: total_length,
        piece_length: piece_length,
        last_piece_index: piece_counts - 1,
        last_piece_length: last_piece_length,
        piece_counts: piece_counts,
        piece_hashes: Enum.map(piece_hash_list, &:binary.list_to_bin/1)
      }

      if length(torrent_data.piece_hashes) != torrent_data.piece_counts do
        raise "Numbers not matched: amount of piece_hash(#{length(torrent_data.piece_hashes)}) vs. amount of piece(#{
                torrent_data.piece_counts
              })"
      end

      # Verify existence of target files
      Enum.each(
        torrent_data.files,
        fn x ->
          if not File.exists?(x["path"]) do
            throw({:file_not_found, x["path"]})
          end
        end
      )

      # Verify content of target files
      bitfield = get_bitfield(progress_file, :list)

      for {val, index} <- Enum.with_index(bitfield), val == 1 do
        piece_data = get_piece_from_disk(torrent_data.files, index, torrent_data.piece_length)

        if :crypto.hash(:sha, piece_data) !== Enum.fetch!(torrent_data.piece_hashes, index) do
          throw({:piece_hash_not_match, index})
        end
      end

      init_state = %FileServer{
        download_dir: download_dir,
        torrent_file: torrent_file,
        progress_file: progress_file,
        trackers_file: trackers_file,
        decoded_torrent: decoded_torrent,
        torrent_data: torrent_data,
        piece_cache: List.duplicate([], torrent_data.piece_counts)
      }

      Logger.info("FileServer initialized: #{inspect(init_state)}")
      {:ok, init_state}
    end
  end

  def calculate_bitfield(torrent_data) do
    # derives bitfield by verifying contents on disk
    Enum.map(
      Enum.with_index(torrent_data.piece_hashes),
      fn piece_hash, piece_index ->
        data = get_piece_from_disk(torrent_data.files, piece_index, torrent_data.piece_length)

        if :crypto.hash(:sha, data) === piece_hash do
          1
        else
          0
        end
      end
    )
  end


  def init_from_new_torrent(download_dir, original_torrent) do
    Logger.info("[FS] init from new torrent #{inspect(original_torrent)}}")
    metadata = File.read!(original_torrent)

    # Prepare job folders
    File.mkdir!(download_dir)
    File.cd!(download_dir)
    job_status_dir = get_job_status_dir()
    File.mkdir!(job_status_dir)

    torrent_file = Path.join(job_status_dir, "job.torrent")
    File.write!(torrent_file, metadata)

    decoded_torrent = parse_torrent_file(torrent_file)
    prepare_files!(download_dir, decoded_torrent)
    init_from_existing_job(download_dir)
  end

  def prepare_files!(download_dir, decoded_torrent) do
    file_info_list = collect_file_info(decoded_torrent)
    Enum.each(
      file_info_list,
      fn x ->
        full_path = Path.join(download_dir, x["path"]) <> ".downloading"
        File.mkdir_p!(Path.dirname(full_path))
        allocate_target_file!(full_path, x["length"])
      end
    )

    progress_file = Path.join(get_job_status_dir(), "progress.txt")
    total_length = List.foldl(file_info_list, 0, fn x, total -> total + x["length"] end)
    piece_counts = :erlang.ceil(total_length / decoded_torrent["info"]["piece length"])
    allocate_progress_file!(progress_file, piece_counts)

    trackers_file = Path.join(get_job_status_dir(), "trackers.txt")
    allocate_trackers_file!(trackers_file)
  end

  def init_from_torrent_info_hash(download_dir, info_hash) do
    Logger.info("[FS] init from info_hash")

    # Prepare job folders
    File.mkdir!(download_dir)
    File.cd!(download_dir)
    job_status_dir = get_job_status_dir()
    File.mkdir!(job_status_dir)
    info_hash_file = Path.join(download_dir, "info_hash.bin")
    File.write!(info_hash_file, info_hash)
    init_from_existing_job(download_dir)
  end

  # =============== GenServer callbacks ===============

  def handle_call(:state, _from, state) do
    # For debugging purpose
    {:reply, state, state}
  end

  def handle_call({:set_event_manager, event_pid}, _from, state) do
    {:reply, :ok, %{state | event_pid: event_pid}}
  end

  def handle_call(:base_dir, _from, state) do
    {:reply, state.download_dir, state}
  end

  def handle_call(:piece_cache, _from, state) do
    {:reply, state.piece_cache, state}
  end

  def handle_call(:metadata_piece_cache, _from, state) do
    {:reply, state.metadata_piece_cache, state}
  end

  def handle_call(:missing_metadata_pieces, _from, state) do
    index_list = for {e, idx} <- Enum.with_index(state.metadata_piece_cache), is_nil(e) do
      idx
    end
    {:reply, index_list, state}
  end

  def handle_call(:size_left, _from, state) do
    # how many bytes "left" for downloading
    size_left =
      length(get_unfinished_pieces(state.progress_file)) * state.torrent_data.piece_length

    {:reply, size_left, state}
  end

  def handle_call(:torrent_data, _from, state) do
    {:reply, state.torrent_data, state}
  end

  def handle_call(:torrent_info_hash, _from, state) do
    info_hash = state.torrent_data.info_hash
    {:reply, info_hash, state}
  end

  def handle_call(:get_piece_info, _from, state) do
    bitfield = get_bitfield(state.progress_file, :list)

    finished =
      for {val, index} <- Enum.with_index(bitfield), val == 1 do
        index
      end

    unfinished =
      for {val, index} <- Enum.with_index(bitfield), val == 0 do
        index
      end

    partial =
      for {val, index} <- Enum.with_index(state.piece_cache), val != [] do
        index
      end

    count = length(bitfield)

    info = %{
      :finished => finished,
      :unfinished => unfinished,
      :partial => partial,
      :count => count,
      :progress => bitfield
    }

    {:reply, info, state}
  end

  def handle_call({:get_piece_info, piece_index}, _from, state) do
    piece_info = %{
      :finished => have_piece?(piece_index, state.progress_file),
      :cached_chunks => Enum.fetch!(state.piece_cache, piece_index),
      :hash => Enum.fetch!(state.torrent_data.piece_hashes, piece_index)
    }

    {:reply, piece_info, state}
  end

  def handle_call({:get_piece_data, piece_index}, _from, state) do
    # Retrieve entire piece data:
    # data -> if found
    # nil -> if not found
    if have_piece?(piece_index, state.progress_file) do
      piece_data = get_piece_from_disk(state.torrent_data.files, piece_index, state.piece_length)
      {:reply, piece_data, state}
    else
      {:reply, nil, state}
    end
  end

  def handle_call({:metadata_size, num_bytes}, _from, state) do
    # Init the empty-cache if it's not initialized
    case state.metadata_piece_cache do
      nil ->
        num_piece = (num_bytes / PeerWireProtocol.metadata_piece_length()) |> :erlang.ceil
        new_cache = List.duplicate(nil, num_piece)
        new_state = %{state | metadata_piece_cache: new_cache}
        {:reply, :ok, new_state}
      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call(:decoded_torrent, _from, state) do
    {:reply, state.decoded_torrent, state}
  end

  def handle_call({:get_piece_chunk, piece_index, begin_offset, chunk_length}, _from, state) do
    # returns:
    #   no data -> nil
    #   data in cache -> cached data
    #   data in disk  -> disk data
    case search_piece_chunk_from_cache(state.piece_cache, piece_index, begin_offset, chunk_length) do
      # No data in cache. Try obtain from file.
      nil ->
        if have_piece?(piece_index, state.progress_file) do
          start_offset = piece_index * state.piece_size + begin_offset
          block_data = read_file_block!(state.swap_file, start_offset, chunk_length)
          {:reply, block_data, state}
        else
          # Cannot find portion in file and cache.
          {:reply, nil, state}
        end

      cached_data ->
        {:reply, cached_data, state}
    end
  end

  def handle_call({:add_piece_chunk, piece_index, begin_offset, chunk_data}, _from, state) do
    # -> :ok | {:ok, :have_piece} | {:error, reason}
    #
    # 1. Verify integrity (offset, size, index, hash)
    # 2. Save a portion of a piece into cache.
    # 3. If it forms a complete piece of data, save the entire piece into disk and remove the corresponding cache
    Logger.debug(
      "[FS] Received piece_chunk #{inspect({piece_index, begin_offset, chunk_data})} size=#{
        byte_size(chunk_data)
      }"
    )

    cond do
      have_piece?(piece_index, state.progress_file) ->
        # Already have this piece on disk
        {:reply, :ok, state}

      piece_index not in 0..(state.torrent_data.piece_counts - 1) ->
        response = {:error, "Non-exist piece index #{inspect(piece_index)}"}
        {:reply, response, state}

      begin_offset not in 0..(state.torrent_data.piece_length - 1) ->
        response = {:error, "Invalid piece begin_offset #{inspect(begin_offset)}"}
        {:reply, response, state}

      begin_offset + byte_size(chunk_data) > state.torrent_data.piece_length ->
        response = {:error, "Chunk size exceed piece_length: #{inspect(byte_size(chunk_data))}"}
        {:reply, response, state}

      true ->
        chunk = {begin_offset, begin_offset + byte_size(chunk_data) - 1, chunk_data}
        piece_cache2 = add_piece_chunk_to_cache(piece_index, chunk, state.piece_cache)

        # Check if this forms a complete piece
        this_piece_size =
          if piece_index == state.torrent_data.piece_counts - 1 do
            # The last piece may have a different piece_length
            case Integer.mod(state.torrent_data.total_length, state.torrent_data.piece_length) do
              0 -> state.torrent_data.piece_length
              r -> r
            end
          else
            state.torrent_data.piece_length
          end

        case Enum.fetch!(piece_cache2, piece_index) do
          [{offset_start, offset_end, piece_data}]
          when offset_start == 0 and offset_end == this_piece_size - 1 and
                 byte_size(piece_data) == this_piece_size ->
            res =
              write_piece_to_disk(
                state.torrent_data.files,
                Enum.fetch!(state.torrent_data.piece_hashes, piece_index),
                piece_index,
                state.torrent_data.piece_length,
                piece_data
              )

            case res do
              :ok ->
                mark_piece_downloaded!(state.progress_file, piece_index)
                notify(state.event_pid, {:have_piece, piece_index})
                # TODO: Check ONLY the corresponding file instead of checking all
                # If the entire file has been downloaded, make sure it has the (.downloading) removed.
                new_file_info_list =
                  Enum.map(
                    state.torrent_data.files,
                    fn file_info ->
                      {_, num_unfinished} = target_file_progress(file_info, state.progress_file)
                      if num_unfinished == 0 and Path.extname(file_info["path"]) == ".downloading" do
                        new_path = Path.rootname(file_info["path"], ".downloading")
                        :ok = File.rename(file_info["path"], new_path)
                        Map.replace!(file_info, "path", new_path)
                      else
                        file_info
                      end
                    end
                  )

                new_torrent_data = %{state.torrent_data | files: new_file_info_list}

                new_state = %{
                  state
                  | piece_cache: List.replace_at(piece_cache2, piece_index, []),
                    torrent_data: new_torrent_data
                }

                Logger.info("[Finished] piece #{piece_index}")
                {:reply, {:ok, :have_piece}, new_state}

              {:error, reason} ->
                Logger.debug("Invalid piece #{piece_index} data: #{inspect(reason)}")
                {:reply, {:error, reason}, state}
            end

          _ ->
            {:reply, :ok, %{state | piece_cache: piece_cache2}}
        end
    end
  end

  def handle_call({:add_metadata_piece, index, data}, _from, state) do
    if state.decoded_torrent != nil do
      # Already have torrent data. Do-nothing.
      {:reply, :ok, state}
    else
      meta_cache = List.replace_at(state.metadata_piece_cache, index, data)
      if Enum.all?(meta_cache) do
        metadata = IO.iodata_to_binary(meta_cache)
        {:ok, _info_dict, info_dict_str} = Bencoding.decode_info_dict(metadata)
        if state.torrent_data.info_hash != :crypto.hash(:sha, info_dict_str) do
          throw("invalid info_hash of torrent metadata")
        else
          torrent_file = Path.join(get_job_status_dir(), "job.torrent")
          File.write!(torrent_file, metadata)
          decoded_torrent = parse_torrent_file(torrent_file)
          prepare_files!(state.download_dir, decoded_torrent)
          {:ok, new_state} = init_from_existing_job(state.download_dir)
          {:reply, :ok, new_state}
        end
      else
        new_state = %{state | metadata_piece_cache: meta_cache}
        {:reply, :ok, new_state}
      end
    end
  end

  def handle_call(:get_bitfield, _from, state) do
    response = get_bitfield(state.progress_file)
    {:reply, response, state}
  end

  def handle_call(:get_unfinished_pieces, _from, state) do
    bitfield = get_bitfield(state.progress_file, :list)

    indices =
      for {val, idx} <- Enum.with_index(bitfield), val == 0 do
        idx
      end

    {:reply, indices, state}
  end

  def handle_call({:get_required_chunks, piece_index}, _from, state) do
    # Returns the missing chunks for forming an entire piece
    # [{offset_start, offset_end}, ...]
    piece_length =
      if piece_index == state.torrent_data.last_piece_index do
        state.torrent_data.last_piece_length
      else
        state.torrent_data.piece_length
      end

    required_chunks =
      case have_piece?(piece_index, state.progress_file) do
        true ->
          []

        false ->
          # find out the "gaps" between each sorted chunks
          cached_chunks = Enum.fetch!(state.piece_cache, piece_index)

          intervals =
            for {start_pos, end_pos, _data} <- cached_chunks do
              {start_pos, end_pos}
            end

          find_gaps(intervals, piece_length)
      end

    {:reply, required_chunks, state}
  end

  def handle_call({:progress, target_file_index}, _from, state) do
    file_info = Enum.fetch!(state.torrent_data.files, target_file_index)
    result = target_file_progress(file_info, state.progress_file)
    {:reply, result, state}
  end

  def handle_call(unknown_msg, from, state) do
    Logger.error("[FS] Unknown handle_call message from #{inspect(from)}: #{inspect(unknown_msg)}")
    {:reply, {:error, :unknown_msg}, state}
  end

  def handle_info(unknown_msg, state) do
    Logger.error("[FS] Unknown handle_info message: #{inspect(unknown_msg)}")
    {:noreply, state}
  end

  # =============== Util-functions ===============
  def target_file_progress(file_info, progress_file) do
    # -> {num_finished_pieces, num_unfinished_pieces}
    num_finished_pieces =
      Enum.count(
        file_info["piece_index_range"],
        fn idx -> have_piece?(idx, progress_file) end
      )

    num_unfinished_pieces =
      Enum.count(
        file_info["piece_index_range"],
        fn idx -> not have_piece?(idx, progress_file) end
      )

    {num_finished_pieces, num_unfinished_pieces}
  end

  def get_job_status_dir() do
    "_job_status"
  end

  def search_piece_chunk_from_cache(cache, piece_index, offset_begin, chunk_length) do
    # Returns chunk data or nil (if not found)
    #
    # cache: [<piece_0_chunks>, <piece_1_chunks>, ...]
    # chunks: [{start_offset, end_offset, data], ...]
    chunks = Enum.fetch!(cache, piece_index)
    # find "segment" that contains the required portion
    case Enum.find(
           chunks,
           fn {chunk_offset_start, _, chunk_data} ->
             offset_begin >= chunk_offset_start and
               offset_begin + chunk_length <= chunk_offset_start + byte_size(chunk_data)
           end
         ) do
      {start_pos, _, data} ->
        :binary.part(data, offset_begin - start_pos, chunk_length)

      nil ->
        nil
    end
  end

  def add_piece_chunk_to_cache(piece_index, new_chunk, piece_cache) do
    # Add given piece chunk to its corresponding chunk list
    # Returns the updated piece_cache
    chunk_list = Enum.fetch!(piece_cache, piece_index)
    chunk_list2 = merge_overlap_chunks([new_chunk | chunk_list])
    List.replace_at(piece_cache, piece_index, chunk_list2)
  end

  def merge_overlap_chunks(chunk_list) do
    # chunk_list = [chunk1, chunk2, ...]
    # chunk = {start, end, data}

    # sort by start_offset
    sorted_chunk_list =
      Enum.sort_by(
        chunk_list,
        fn {start, _, _} -> start end
      )

    Enum.reduce_while(sorted_chunk_list, [], fn incoming_chunk, processed_chunks ->
      acc =
        case processed_chunks do
          [] ->
            [incoming_chunk]

          [last_chunk | rest] ->
            case merge_chunks(last_chunk, incoming_chunk) do
              {:ok, merged_chunk} ->
                [merged_chunk | rest]

              {:error, :not_continuous_segments} ->
                [incoming_chunk | processed_chunks]
            end
        end

      {:cont, acc}
    end)
  end

  def merge_chunks(chunk1, chunk2) do
    # Merge two binary chunks:
    #   chunk = {start_pos, end_pos, data}
    # Returns:
    #   {:ok, merged_chunk} if the offsets forms a continuous segment
    #   {:error, :not_continuous_segments} if not.
    {start_1, end_1, data_1} = chunk1
    {start_2, end_2, data_2} = chunk2
    Logger.debug("[FS] merge chunks: {#{start_1}, #{end_1}} and {#{start_2}, #{end_2}}")

    cond do
      start_1 > start_2 ->
        merge_chunks(chunk2, chunk1)

      end_1 + 1 < start_2 ->
        {:error, :not_continuous_segments}

      end_1 >= end_2 ->
        # chunk1 covers chunk2
        {:ok, chunk1}

      true ->
        merged_chunk = {
          start_1,
          end_2,
          :binary.part(data_1, 0, start_2 - start_1) <> data_2
        }

        Logger.debug("[FS] merge chunks result #{inspect(merged_chunk)}")
        {:ok, merged_chunk}
    end
  end

  def test_merge_chunks() do
    {:error, :not_continuous_segments} =
      merge_chunks(
        {0, 2, <<0, 1, 2>>},
        {4, 5, <<4, 5>>}
      )

    {:ok, {0, 3, <<0, 1, 2, 3>>}} =
      merge_chunks(
        {0, 1, <<0, 1>>},
        {2, 3, <<2, 3>>}
      )

    {:ok, {2, 7, <<2, 3, 4, 5, 6, 7>>}} =
      merge_chunks(
        {3, 7, <<3, 4, 5, 6, 7>>},
        {2, 5, <<2, 3, 4, 5>>}
      )

    {:ok, {2, 6, <<2, 3, 4, 5, 6>>}} =
      merge_chunks(
        {2, 6, <<2, 3, 4, 5, 6>>},
        {3, 5, <<3, 4, 5, 6>>}
      )
  end

  def intersect(x_start..x_end, y_start..y_end) do
    cond do
      x_end < x_start -> raise("x_end cannot be smaller than x_start")
      y_end < y_start -> raise("y_end cannot be smaller than y_start")
      x_start > y_end -> nil
      y_start > x_end -> nil
      true -> max(x_start, y_start)..min(x_end, y_end)
    end
  end

  def test_intersect() do
    nil = intersect(1..2, 3..4)
    nil = intersect(3..4, 1..2)
    1..1 = intersect(0..1, 1..2)
    2..3 = intersect(-1..3, 2..999)
    2..3 = intersect(2..999, -1..3)
    3..4 = intersect(3..4, -1..100)
  end

  def piece_locations(piece_index, piece_length, file_info_list) do
    # Given a piece, find its corresponding file-offsets
    # -> [{filepath, offset_interval}, ...]

    total_offset_start = piece_index * piece_length

    total_offset_end = total_offset_start + piece_length - 1
    piece_range = total_offset_start..total_offset_end

    reversed_loc =
      List.foldl(file_info_list, [], fn x, acc ->
        file_range = x["offset_start"]..(x["offset_start"] + x["length"] - 1)

        case intersect(piece_range, file_range) do
          nil ->
            acc

          off_start..off_end = _interval ->
            overlap = (off_start - x["offset_start"])..(off_end - x["offset_start"])
            [{x["path"], overlap} | acc]
        end
      end)

    Enum.reverse(reversed_loc)
  end

  def test_piece_locations() do
    data1 = [
      %{"path" => "a.txt", "offset_start" => 0, "length" => 8},
      %{"path" => "b.txt", "offset_start" => 8, "length" => 2}
    ]

    [{"a.txt", 5..7}, {"b.txt", 0..1}] = piece_locations(1, 5, data1)

    data2 = [
      %{"path" => "a.txt", "offset_start" => 0, "length" => 7},
      %{"path" => "b.txt", "offset_start" => 7, "length" => 2},
      %{"path" => "c.txt", "offset_start" => 9, "length" => 1},
      %{"path" => "d.txt", "offset_start" => 10, "length" => 5}
    ]

    [{"a.txt", 5..6}, {"b.txt", 0..1}, {"c.txt", 0..0}] = piece_locations(1, 5, data2)
  end

  def have_piece?(piece_index, progress_file) do
    # If we already have this piece in disk

    bitfield = get_bitfield(progress_file, :list)

    case Enum.fetch!(bitfield, piece_index) do
      0 -> false
      1 -> true
    end
  end

  def get_piece_from_disk(file_info_list, piece_index, piece_length) do
    locations = piece_locations(piece_index, piece_length, file_info_list)

    piece_data =
      List.foldl(locations, <<"">>, fn {filepath, start_pos..end_pos}, acc ->
        data = read_file_block!(filepath, start_pos, end_pos - start_pos + 1)
        acc <> data
      end)

    piece_data
  end

  def write_piece_to_disk(file_info_list, piece_hash, piece_index, piece_length, piece_data) do
    # returns :ok | {:error, reason}
    # 1. verify piece size == data size
    # 2. verify piece hash
    # 3. write data to file
    cond do
      :crypto.hash(:sha, piece_data) !== piece_hash ->
        {:error, :hash_not_matched}

      true ->
        locations = piece_locations(piece_index, piece_length, file_info_list)
        Logger.debug("Writing piece #{piece_index} into locations: #{inspect(locations)}")

        <<"">> =
          List.foldl(locations, piece_data, fn {filepath, start_pos..end_pos}, data ->
            data_size = end_pos - start_pos + 1
            <<block::binary-size(data_size), rest_data::binary>> = data
            write_file_block!(filepath, start_pos, block)
            rest_data
          end)

        Logger.info("[Finished] write piece #{piece_index} into disk")
        :ok
    end
  end

  def test_find_gaps() do
    [] = find_gaps([{0, 4}], 5)
    [{2, 4}] = find_gaps([{0, 1}], 5)
    [{0, 0}, {2, 2}] = find_gaps([{1, 1}, {3, 4}], 5)
    [{2, 2}, {4, 4}] = find_gaps([{0, 1}, {3, 3}], 5)
  end

  def find_gaps(intervals, piece_length) do
    r =
      List.foldl(intervals, {[], -1}, fn {start_pos, end_pos}, {gaps, last_end} ->
        cond do
          start_pos > last_end + 1 ->
            {[{last_end + 1, start_pos - 1} | gaps], end_pos}

          start_pos == last_end + 1 ->
            {gaps, end_pos}
        end
      end)

    {gaps_found, last_interval_end} = r

    cond do
      last_interval_end == piece_length - 1 ->
        Enum.reverse(gaps_found)

      last_interval_end < piece_length - 1 ->
        last_gap = {last_interval_end + 1, piece_length - 1}
        Enum.reverse([last_gap | gaps_found])
    end
  end

  def get_unfinished_pieces(progress_file) do
    # Returns a list of unfinished piece-index
    # e.g. [0,1,7]
    bitfield = get_bitfield(progress_file, :list)

    unfinished_piece_indices =
      for {val, i} <- Enum.with_index(bitfield), val === 0 do
        i
      end

    unfinished_piece_indices
  end

  def allocate_progress_file!(filepath, num_of_pieces) do
    Logger.info("[FS] allocate progress file #{filepath}")
    File.touch!(filepath)
    bitfield = List.duplicate(0, num_of_pieces)
    set_bitfield!(filepath, bitfield)
  end

  def allocate_trackers_file!(filepath) do
    File.touch!(filepath)
  end

  def allocate_target_file!(swap_file, file_size) do
    Logger.info("[FS] allocate target file #{swap_file}. size #{file_size}}")
    case :os.type() do
      {:unix, :linux} ->
        {_, 0} = System.cmd("fallocate", ["--length", Integer.to_string(file_size), swap_file])

      {:unix, _others} ->
        {_, 0} = System.cmd("mkfile", ["-nv", Integer.to_string(file_size), swap_file])
    end
  end

  def parse_torrent_file(filepath, decode_dict_as \\ :maps) do
    content = File.read!(filepath)
    data = Bencoding.decode(content, %{decode_dict_as: decode_dict_as, decode_string_as: :utf8})
    Logger.debug("Parsed torrent: #{inspect(data)}")
    data
  end

  def collect_file_info(decoded_torrent) do
    # This function is used to generate consistent representation of file-info
    # for both single-file and multi-file torrent
    # https://wiki.theory.org/index.php/BitTorrentSpecification#Info_Dictionary
    #
    # Returns: [file1_info, file2_info, ...]
    # Where file_info = %{"length"=> file size,
    #                      "path"=> relative file path,
    #                      "offset_start" => Byte offset of the ENTIRE download content}
    #                      "piece_index_range" => occupied piece index in range

    piece_length = decoded_torrent["info"]["piece length"]

    file_info_list =
      if Map.has_key?(decoded_torrent["info"], "files") do
        # multi-file torrent
        file_dir = decoded_torrent["info"]["name"]

        {_, info_list} =
          List.foldl(
            decoded_torrent["info"]["files"],
            {0, []},
            fn x, {acc_offset, info_list} ->
              info = %{
                "length" => x["length"],
                "path" => Path.join([file_dir | x["path"]]),
                "offset_start" => acc_offset
              }

              piece_index_start = div(info["offset_start"], piece_length)
              piece_index_end = div(info["offset_start"] + x["length"], piece_length)
              info2 = Map.put(info, "piece_index_range", piece_index_start..piece_index_end)
              {acc_offset + info["length"], [info2 | info_list]}
            end
          )

        Enum.reverse(info_list)
      else
        # single-file torrent
        [
          %{
            "length" => decoded_torrent["info"]["length"],
            "path" => decoded_torrent["info"]["name"],
            "offset_start" => 0
          }
        ]
      end

    file_info_list
  end

  def get_bitfield(progress_file, format \\ :bitstring) do
    # -> [1,0,0,..]
    # -> 0b100... (bitstring with padding zeros)
    # -> nil

    case format do
      :list ->
        case File.read(progress_file) do
          {:ok, data} ->
            # "01011" => ["0","1","0","1","1"]
            list_of_char = String.graphemes(data)
            Enum.map(list_of_char, &String.to_integer/1)

          {:error, :enoent} ->
            nil
        end

      :bitstring ->
        case get_bitfield(progress_file, :list) do
          nil ->
            nil

          x when is_list(x) ->
            num_of_padding_bits = rem(length(x), 8)

            paddings =
              if num_of_padding_bits == 0 do
                []
              else
                List.duplicate(0, 8 - num_of_padding_bits)
              end

            List.foldl(
              x ++ paddings,
              <<0::0>>,
              fn e, acc ->
                case e do
                  0 -> <<acc::bitstring, 0::1>>
                  1 -> <<acc::bitstring, 1::1>>
                end
              end
            )
        end

      _ ->
        raise "arg 'format' must be either :bitstring or :list"
    end
  end

  def mark_piece_downloaded!(progress_file, piece_index) do
    offset = piece_index
    data = Integer.to_string(1)
    write_file_block!(progress_file, offset, data)
  end

  def set_bitfield!(progress_file, bitfield) when is_bitstring(bitfield) do
    bitfield_in_list =
      for <<i::1 <- bitfield>> do
        i
      end

    set_bitfield!(progress_file, bitfield_in_list)
  end

  def set_bitfield!(progress_file, bitfield) when is_list(bitfield) do
    # [0,0,1,0,0,1] --> "001001" --(write)-> progress_file
    bitfield_str = Enum.join(bitfield)
    File.write!(progress_file, bitfield_str)
  end

  def get_download_progress(progress_file) do
    # -> :finished | :partial | :not_initialized
    bitfield = get_bitfield(progress_file, :list)

    cond do
      bitfield === nil -> :not_initialized
      Enum.member?(bitfield, 0) -> :partial
      true -> :finished
    end
  end

  def calculate_torrent_info_hash(torrent_file) do
    {:ok, _info_dict, info_dict_str} = Bencoding.decode_info_dict(File.read!(torrent_file))
    :crypto.hash(:sha, info_dict_str)
  end

  def read_file_block!(filepath, offset_start, block_length_bytes) do
    Logger.debug("Reading file #{filepath} offset #{offset_start} length #{block_length_bytes}")
    {:ok, f} = :file.open(filepath, [:binary])

    try do
      {:ok, data} = :file.pread(f, offset_start, block_length_bytes)
      data
    after
      :file.close(f)
    end
  end

  def write_file_block!(filepath, offset_start, block_data) do
    Logger.debug(
      "Writing to file #{filepath}, offset_start:#{offset_start}, data size:#{
        byte_size(block_data)
      }"
    )

    {:ok, f} = :file.open(filepath, [:read, :write, :binary])

    try do
      :ok = :file.pwrite(f, offset_start, block_data)
    after
      :file.close(f)
    end
  end

  def notify(event_pid, event) do
    if event_pid do
      send(event_pid, event)
    else
      Logger.debug("(skipped) #{inspect(event)}: event_pid not set")
    end
  end
end

defmodule Bencoding do
  require Logger
  # @type B_any() ::
  #                 | B_str
  #                 | B_int
  #                 | B_list
  #                 | B_dict

  # @type B_dict() :: d(<B_str><B_any>)*e

  # @type B_str() :: <int>:<char length:int>

  # @type B_int() :: i<int>e

  # @type B_list() :: l<B_any>*e

  def log(_data, _args \\ [label: ""]) do
    # IO.inspect data, label: args[:label]
    # Logger.info("[Bencoding:#{args[:label]}] #{inspect data}")
  end

  def test_all() do
    test_decode()
    test_encode()

    decode(File.read!("test/curl_output.bin"))
  end

  def decode(s, option \\ %{}) do
    default_opt = %{decode_string_as: :ascii}
    opt = Map.merge(default_opt, option)
    {b_obj, ""} = decode_prefix(s, opt)
    b_obj
  end

  def decode_info_dict(s) do
    # Extract only the info-dict and its original Bencoded string.
    # -> {:error, reason}
    # -> {:ok, decoded_info_dict, original_str_of_info_dict}
    if not is_map(decode(s)["info"]) do
      {:error, "Invalid torrent: cannot find torrent info-dict"}
    else
      try do
        decode(s, %{extract_info_dict: true})
      catch
        {:info_dict, dict, original_str} ->
          {:ok, dict, original_str}
      end
    end
  end

  def decode_prefix(s, option) do
    {b_obj, rest} =
      case String.first(s) do
        nil -> raise "Attempt to decode an empty string"
        "d" -> decode_prefix_dict(s, option)
        "i" -> decode_prefix_int(s, option)
        "l" -> decode_prefix_list(s, option)
        _ -> decode_prefix_str(s, option)
      end

    {b_obj, rest}
  end

  def decode_prefix_dict(s, option) do
    log(s, label: "decode dict")
    log(option, label: "decode dict option")
    # Decode dict into keyword list (to preserve the ordering from bencoded-string)
    {dict, remain} =
      case s do
        "de" <> tail ->
          {%{}, tail}

        "d" <> tail ->
          {dict_key, rest} = decode_prefix_str(tail, option)

          {dict_obj, rest2} =
            case dict_key do
              "pieces" ->
                # This is a special case for torrent file
                # The content of pieces is SHA-1 values, not unicode
                decode_prefix_str(rest, %{option | decode_string_as: :ascii})

              _ ->
                decode_prefix(rest, option)
            end

          {rest_map, rest3} = decode_prefix_dict("d" <> rest2, option)
          m = Map.merge(%{dict_key => dict_obj}, rest_map)

          if dict_key == "info" and option[:extract_info_dict] do
            info_dict_str = binary_part(rest, 0, byte_size(rest) - byte_size(rest3) - 1)
            throw({:info_dict, m, info_dict_str})
          end

          {m, rest3}
      end

    log(remain, lable: "decode dict remain")
    {dict, remain}
  end

  def decode_prefix_list(s, option) do
    log(s, label: "decode list")

    case s do
      "le" <> rest ->
        {[], rest}

      "l" <> tail ->
        case decode_prefix(tail, option) do
          {b_obj, "e" <> rest2} ->
            {[b_obj], rest2}

          {b_obj, rest2} ->
            {b_list, rest3} = decode_prefix_list("l" <> rest2, option)
            {[b_obj | b_list], rest3}
        end
    end
  end

  def decode_prefix_int(s, _option) do
    log(s, label: "decode int")

    %{"int_string" => int_string, "rest" => rest} =
      Regex.named_captures(~r/\Ai(?<int_string>0|[-]?[1-9]+[0-9]*)e(?<rest>.*)\z/sm, s)

    log({String.to_integer(int_string), rest}, label: "decode int got")
    {String.to_integer(int_string), rest}
  end

  def decode_prefix_str(s, option) do
    log(s, label: "decode str")
    %{"str_len" => str_len} = Regex.named_captures(~r/\A(?<str_len>0|[1-9]+[0-9]*):/sm, s)
    string_length = String.to_integer(str_len)
    log(string_length, label: "str len")

    result =
      case option[:decode_string_as] do
        :utf8 ->
          {_, rest} = String.split_at(s, String.length(str_len) + 1)

          if string_length > String.length(rest) do
            raise "length of string (UTF-8) not matched: #{inspect(rest)}(#{String.length(rest)}) but expect #{
                    string_length
                  }"
          end

          {b_str, rest2} = String.split_at(rest, string_length)
          {b_str, rest2}

        :ascii ->
          {_, rest} = String.split_at(s, String.length(str_len) + 1)

          cond do
            string_length === 0 ->
              {"", rest}

            string_length > byte_size(rest) ->
              raise "length of string (ASCII) not matched: #{inspect(rest)}(#{String.length(rest)}) but expect #{
                      string_length
                    }"

            true ->
              <<b_str::binary-size(string_length), rest2::binary>> = rest
              {b_str, rest2}
          end
      end

    log(result, label: "got str")
    result
  end

  def examples() do
    [
      # decode string
      {"", "0:"},
      {"abc", "3:abc"},
      # decode int
      {0, "i0e"},
      {3, "i3e"},
      {-300, "i-300e"},
      # decode list
      {[], "le"},
      {["spam", "eggs"], "l4:spam4:eggse"},
      # decode dictionary => map
      {%{}, "de"},
      {%{"cow" => "moo", "spam" => "eggs"}, "d3:cow3:moo4:spam4:eggse"},
      {%{"spam" => ["a", "b"]}, "d4:spaml1:a1:bee"},
      {%{
         "publisher" => "bob",
         "publisher-webpage" => "www.example.com",
         "publisher.location" => "home"
       }, "d9:publisher3:bob17:publisher-webpage15:www.example.com18:publisher.location4:homee"}
    ]
  end

  def test_decode() do
    Enum.map(
      examples(),
      fn {after_decode, before_decode} ->
        # Logger.debug "test decoding #{inspect before_decode} => #{inspect after_decode}"
        if decode(before_decode) !== after_decode do
          raise "decode not match: #{after_decode}, #{before_decode}"
        end
      end
    )
  end

  def test_encode() do
    Enum.map(
      examples(),
      fn {after_decode, before_decode} ->
        r = encode(after_decode)

        if r !== before_decode do
          raise "encode not match: #{inspect(after_decode)},
                                #{inspect(before_decode)}, got #{inspect(r)}"
        end
      end
    )
  end

  def encode(target) do
    case target do
      x when is_bitstring(x) -> encode_str(x)
      x when is_list(x) -> encode_list(x)
      x when is_map(x) -> encode_dict(x)
      x when is_integer(x) -> encode_int(x)
      _ -> raise("Un-supported type for encoding: #{inspect(target)}")
    end
  end

  def encode_str(target, encode_as \\ :ascii) when is_bitstring(target) do
    case encode_as do
      :utf8 -> "#{String.length(target)}:#{target}"
      :ascii -> "#{byte_size(target)}:#{target}"
    end
  end

  def encode_int(target) when is_integer(target) do
    "i#{target}e"
  end

  def encode_dict(target) when is_map(target) do
    # The key must be sorted in alphabetical order before encoding
    # The strings should be compared using a binary comparison, not a culture-specific "natural" comparison
    sorted_kwlist =
      Enum.sort(
        Map.to_list(target),
        fn {k1, _v1}, {k2, _v2} ->
          k1 <= k2
        end
      )

    #        log sorted_kwlist, label: "sorted kwlist"
    encoded_list =
      Enum.reduce(
        sorted_kwlist,
        [],
        fn {key, val}, acc ->
          acc ++ [encode(key), encode(val)]
        end
      )

    "d#{Enum.join(encoded_list)}e"
  end

  def encode_list(target) when is_list(target) do
    str_joined = Enum.map(target, &encode/1) |> Enum.join()
    "l#{str_joined}e"
  end
end

defmodule Connector do
  use GenServer
  require Logger

  defstruct torrent_data: %{},
            # ==== torrent info (won't change) ====

            # ==== client running status ====
            my_p2p_id: nil,
            socket: nil,
            status: :new,
            raw_packet_received: nil,
            stats: %{received: 0, sent: 0},
            support: [:extension_protocol],
            created_at: nil,

            # ==== file status ====
            i_requested_pieces: [],
            fileserver_pid: nil,

            # ==== connection status ====
            am_choked: true,
            am_interested: false,
            choking_peer: true,
            interested_at_peer: false,
            received_handshake: false,
            sent_handshake: false,
            keepalive_timer: nil,

            # ==== Peer-related info ====
            peer_id: nil,
            peer_ip: nil,
            peer_port: nil,
            peer_requested_chunks: [],
            peer_has_pieces: MapSet.new(),
            peer_last_active_time: nil,
            peer_support: [],
            peer_extend_msg_mappings: %{}

  def start(init_args) do
    GenServer.start(__MODULE__, init_args, [])
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, [])
  end

  def do_log(level, msg, state) do
    prefix = "[Connector #{inspect(state.peer_ip)}:#{state.peer_port}]: "
    Logger.log(level, prefix <> msg)
  end

  def init([socket, torrent_data, my_p2p_id, peer_id, fileserver_pid]) do
    :ok = :inet.setopts(socket, active: true)
    {:ok, {peer_ip, peer_port}} = :inet.peername(socket)

    state = %Connector{
      peer_id: peer_id,
      peer_ip: peer_ip,
      peer_port: peer_port,
      my_p2p_id: my_p2p_id,
      fileserver_pid: fileserver_pid,
      torrent_data: torrent_data,
      peer_last_active_time: System.system_time(:second),
      socket: socket,
      created_at: System.system_time(:second)
    }

    Logger.debug("Connector init: #{inspect(state)}")
    {:ok, state, {:continue, :after_connect}}
  end

  def init([peer_ip, peer_port, torrent_data, my_p2p_id, peer_id, fileserver_pid]) do
    # initialize state and attempt to connect to peer
    # inside handle_continue/2

    state = %Connector{
      peer_id: peer_id,
      peer_ip: peer_ip,
      peer_port: peer_port,
      my_p2p_id: my_p2p_id,
      fileserver_pid: fileserver_pid,
      torrent_data: torrent_data,
      created_at: System.system_time(:second)
    }

    Logger.debug("[gen_tcp] Connecting to #{inspect(state.peer_ip)}:#{inspect(state.peer_port)} ...")

    case :gen_tcp.connect(state.peer_ip, state.peer_port, [:binary, active: true], 10000) do
      {:ok, socket} ->
        Logger.info("[gen_tcp] Connected to #{inspect(state.peer_ip)}:#{state.peer_port}")
        s2 = %{state | socket: socket, peer_last_active_time: System.system_time(:second)}
        {:ok, s2, {:continue, :after_connect}}
      {:error, :timeout} ->
        {:stop, {:shutdown, {:gen_tcp_connect, :timeout}}, state}
      {:error, reason} ->
        {:stop, {:gen_tcp_connect, reason}, state}
    end
  end

  ##########################################################################
  #                GenServer Callbacks
  ##########################################################################

  def handle_continue(:after_connect, state) do
    # This is the phase of performing handshakes
    s1 = do_handshake(state)
    Process.sleep(3000)

    s2 = do_extension_handshake(s1)

    msg_interested = PeerWireProtocol.encode(:interested)
    send_binary_data_to_peer(msg_interested, state.socket)

    msg_unchoke = PeerWireProtocol.encode(:unchoke)
    send_binary_data_to_peer(msg_unchoke, state.socket)

    bitfield = GenServer.call(state.fileserver_pid, :get_bitfield)
    msg_bitfield = PeerWireProtocol.encode(:bitfield, bitfield)
    send_binary_data_to_peer(msg_bitfield, state.socket)

    send(self(), :keepalive_loop)
    new_state = %{s2 | choking_peer: false, interested_at_peer: true}
    {:noreply, new_state}
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:stats, _from, state) do
    stats = %{
      uptime: System.system_time(:second) - state.created_at
    }

    {:reply, stats, state}
  end

  def handle_call(:peer_info, _from, state) do
    peer_info = %{
      :id => state.peer_id,
      :ip => state.peer_ip,
      :port => state.peer_port
    }

    {:reply, peer_info, state}
  end

  def handle_call(:peer_has_pieces, _from, state) do
    {:reply, state.peer_has_pieces, state}
  end

  def handle_call(:handshaked, _from, state) do
    resp = state.sent_handshake and state.received_handshake
    {:reply, resp, state}
  end

  def handle_call(:enable_keepalive, _from, state) do
    send(self(), :keepalive)
    Logger.debug("enable keepalive")
    {:reply, :ok, state}
  end

  def handle_call(:peer_requested_chunks, _from, state) do
    {:reply, state.peer_requested_chunks, state}
  end

  def handle_call({:send_chunk, piece_index, begin_offset, chunk_length}, _from, state) do
    # Get chunk data from fileserver then send it to peer

    chunk_data =
      GenServer.call(
        state.fileserver_pid,
        {:get_piece_chunk, piece_index, begin_offset, chunk_length}
      )

    resp =
      case chunk_data do
        nil ->
          Logger.warn("handle_call(:send_chunk): Chunk {} not found. Skipped.")
          {:error, :chunk_not_found}

        _ ->
          msg = PeerWireProtocol.encode(:piece, piece_index, begin_offset, chunk_data)
          send_binary_data_to_peer(msg, state.socket)
      end

    new_state = %{
      state
      | peer_requested_chunks:
          List.delete(
            state.peer_requested_chunks,
            {piece_index, begin_offset, chunk_length}
          )
    }

    {:reply, resp, new_state}
  end

  def handle_call({:send_msg, bin_data}, from, state) when is_binary(bin_data) do
    msg = PeerWireProtocol.decode!(bin_data)
    me = self()

    spawn_link(fn ->
      result = GenServer.call(me, {:send_msg, msg})
      GenServer.reply(from, result)
    end)

    {:noreply, state}
  end

  def handle_call(:send_handshake, _from, state) do
    # handshake: <pstrlen><pstr><reserved><info_hash><peer_id>
    new_state = do_handshake(state)
    {:reply, :ok, new_state}
  end

  def handle_call(:send_extension_handshake, _from, state) do
    new_state = do_extension_handshake(state)
    {:reply, :ok, new_state}
  end

  def handle_call({:request_piece, piece_index, begin_offset, chunk_length}, _from, state) do
    if state.am_choked === true do
      Logger.debug("[CN] Skip request: I am choked !!")
      me = self()
      spawn(fn -> GenServer.call(me, {:send_msg, :interested}) end)
      {:reply, {:error, :i_am_choked}, state}
    else
      length_limit = PeerWireProtocol.max_piece_request_length()

      if chunk_length > length_limit do
        Logger.warn("Requested chunk too large (#{chunk_length}), reduced to #{length_limit}")
        msg = PeerWireProtocol.encode(:request, piece_index, begin_offset, length_limit)
        :ok = send_binary_data_to_peer(msg, state.socket)
        {:reply, :ok, state}
      else
        msg = PeerWireProtocol.encode(:request, piece_index, begin_offset, chunk_length)
        :ok = send_binary_data_to_peer(msg, state.socket)
        {:reply, :ok, state}
      end
    end
  end

  def handle_call({:request_metadata_piece, piece_index}, _from, state) do
    case state.peer_extend_msg_mappings["ut_metadata"] do
      nil ->
        # Peer not supporting ut_metadata. Do-nothing.
        :noop
      extend_msg_id ->
        # TODO: refactor
        data = %{"msg_type" => 0, "piece" => piece_index}
        raw_data = Bencoding.encode(data)
        bin_msg = PeerWireProtocol.encode(:extended, :raw, extend_msg_id, raw_data)
        :ok = send_binary_data_to_peer(bin_msg, state.socket)
    end
    {:reply, :ok, state}
  end

  def handle_call({:send_msg, :keepalive}, _from, state) do
    msg = PeerWireProtocol.encode(:keepalive)
    :ok = send_binary_data_to_peer(msg, state.socket)
    {:reply, :ok, state}
  end

  def handle_call({:send_msg, :choke}, _from, state) do
    msg = PeerWireProtocol.encode(:choke)
    :ok = send_binary_data_to_peer(msg, state.socket)
    new_state = %{state | choking_peer: true}
    {:reply, :ok, new_state}
  end

  def handle_call({:send_msg, :unchoke}, _from, state) do
    msg = PeerWireProtocol.encode(:unchoke)
    :ok = send_binary_data_to_peer(msg, state.socket)
    new_state = %{state | choking_peer: false}
    {:reply, :ok, new_state}
  end

  def handle_call({:send_msg, :interested}, _from, state) do
    msg = PeerWireProtocol.encode(:interested)
    :ok = send_binary_data_to_peer(msg, state.socket)
    new_state = %{state | interested_at_peer: true}
    {:reply, :ok, new_state}
  end

  def handle_call({:send_msg, :not_interested}, _from, state) do
    msg = PeerWireProtocol.encode(:not_interested)
    :ok = send_binary_data_to_peer(msg, state.socket)
    new_state = %{state | interested_at_peer: false}
    {:reply, :ok, new_state}
  end

  def handle_call({:send_msg, {:have, piece_index}}, _from, state) do
    msg = PeerWireProtocol.encode(:have, piece_index)
    :ok = send_binary_data_to_peer(msg, state.socket)
    {:reply, :ok, state}
  end

  def handle_call({:send_msg, {:bitfield, bitfield}}, _from, state) when is_binary(bitfield) do
    Logger.debug("[PeerWireProtocol - bitfield] send my bitfield #{inspect(bitfield)}")
    msg = PeerWireProtocol.encode(:bitfield, bitfield)
    :ok = send_binary_data_to_peer(msg, state.socket)
    {:reply, :ok, state}
  end

  def handle_call({:send_msg, {:request, piece_index, begin_offset, chunk_length}}, _from, state) do
    Logger.debug(
      "[PeerWireProtocol - request] request piece #{piece_index} #{begin_offset} #{chunk_length}"
    )

    msg = PeerWireProtocol.encode(:request, piece_index, begin_offset, chunk_length)
    :ok = send_binary_data_to_peer(msg, state.socket)
    {:reply, :ok, state}
  end

  def handle_call({:send_msg, {:piece, piece_index, begin_offset, chunk_data}}, _from, state)
      when is_binary(chunk_data) do
    msg = PeerWireProtocol.encode(:piece, piece_index, begin_offset, chunk_data)
    :ok = send_binary_data_to_peer(msg, state.socket)
    chunk_length = byte_size(chunk_data)
    chunks = List.delete(state.peer_requested_chunks, {piece_index, begin_offset, chunk_length})
    new_stats = %{state.stats | sent: state.stats.sent + byte_size(chunk_data)}
    new_state = %{state | peer_requested_chunks: chunks, stats: new_stats}
    {:reply, :ok, new_state}
  end

  def handle_call({:send_msg, {:cancel, piece_index, offset_begin, chunk_length}}, _from, state) do
    msg = PeerWireProtocol.encode(:cancel, piece_index, offset_begin, chunk_length)
    :ok = send_binary_data_to_peer(msg, state.socket)
    {:reply, :ok, state}
  end

  def handle_call({:send_msg, {:port, listen_port}}, _from, state) do
    msg = PeerWireProtocol.encode(:port, listen_port)
    :ok = send_binary_data_to_peer(msg, state.socket)
    {:reply, :ok, state}
  end

  def handle_call(msg, from, state) do
    msg = "Unknown handle_call msg: #{inspect(msg)} from: #{inspect(from)}"
    Logger.error(msg)
    {:reply, {:error, msg}, state}
  end

  def handle_cast({:received_msg, peer_msg}, state) do
    # Received peer message
    Logger.debug("On received peer_msg #{inspect(peer_msg)}")
    # update peer's last alive timestamp
    state2 = %{state | peer_last_active_time: System.system_time(:second)}

    # Must passed handshake before proceed
    if state.received_handshake === false and not is_handshake_msg?(peer_msg) do
      {:stop, {"Receive non-handshake message when started", peer_msg}, state}
    else
      on_received_peer_msg(peer_msg, state2)
    end
  end

  def handle_info(:keepalive_loop, state) do
    # Checks all kinds of keepalive conditions

    inactive_time = System.system_time(:second) - state.peer_last_active_time

    if inactive_time > PeerWireProtocol.keepalive_interval_sec() do
      {:stop, {:noresponse, inactive_time}, state}
    else
      if state.keepalive_timer do
        Process.cancel_timer(state.keepalive_timer)
      end

      keepalive_msg = PeerWireProtocol.encode(:keepalive)
      :ok = send_binary_data_to_peer(keepalive_msg, state.socket)
      Logger.debug("[Sent] :keepalive")

      timer =
        Process.send_after(
          self(),
          :keepalive_loop,
          PeerWireProtocol.keepalive_interval_sec() * 500
        )

      new_state = %{state | keepalive_timer: timer}
      {:noreply, new_state}
    end
  end

  def handle_info({:tcp, _socket, packet}, state) do
    # Dealing with incoming TCP packets

    Logger.debug("Incoming packet #{inspect(packet)}")

    buffer =
      case state.raw_packet_received do
        nil -> packet
        _ -> state.raw_packet_received <> packet
      end

    new_state = do_decode_tcp(%{state | raw_packet_received: buffer})

    Logger.debug(
      "Buffer remain: size #{byte_size(new_state.raw_packet_received)}, data #{
        inspect(new_state.raw_packet_received)
      }"
    )

    {:noreply, new_state}
  end

  def handle_info({:tcp_closed, socket}, state) do
    msg = "TCP Closed: #{inspect(:inet.peername(socket))}"
    Logger.info(msg)
    reason = {:tcp_closed, socket}
    {:stop, {:shutdown, reason}, state}
  end

  def handle_info({:tcp_error, socket, reason}, state) do
    msg = "TCP Error: #{inspect(reason)}. socket = #{inspect(:inet.peername(socket))}"

    Logger.error(msg)
    reason = {:tcp_error, socket, reason}
    {:stop, reason, state}
  end

  def handle_info(msg, state) do
    Logger.error("got unknown handle_info/2 msg #{inspect(msg)}")
    {:noreply, state}
  end

  ##########################################################
  # Module Functions
  ##########################################################

  def do_handshake(state) do
    info_hash = GenServer.call(state.fileserver_pid, :torrent_info_hash)
    msg = PeerWireProtocol.encode(:handshake, info_hash, state.my_p2p_id, state.support)
    Logger.debug("Send handshake (HEX): #{Base.encode16(msg)}")
    :ok = send_binary_data_to_peer(msg, state.socket)
    %{state | sent_handshake: true}
  end

  def do_extension_handshake(state) do
    msg_mappings = %{"ut_metadata" => 3}
    info = %{"m" => msg_mappings}
    msg = PeerWireProtocol.encode(:extended, :handshake, info)
    :ok = send_binary_data_to_peer(msg, state.socket)
    state
  end

  def is_handshake_msg?(msg) do
    case msg do
      {:handshake, _, _, _} -> true
      _ -> false
    end
  end

  def do_decode_tcp(state) do
    Logger.debug("Checking tcp buffer: #{inspect(state.raw_packet_received)}")

    if not state.received_handshake do
      # 68: size of handshake message
      if byte_size(state.raw_packet_received) < 68 do
        state
      else
        {:ok, peer_msg, rest_buffer} = PeerWireProtocol.decode_one(state.raw_packet_received)
        GenServer.cast(self(), {:received_msg, peer_msg})
        new_state = %{state | raw_packet_received: rest_buffer}
        do_decode_tcp(new_state)
      end
    else
      # Have received handshake msg.
      size_ptr_len = PeerWireProtocol.msg_size_ptr_length() * 8

      if bit_size(state.raw_packet_received) < size_ptr_len do
        state
      else
        <<size_ptr::size(size_ptr_len), rest::binary>> = state.raw_packet_received

        if byte_size(rest) < size_ptr do
          state
        else
          case PeerWireProtocol.decode_one(state.raw_packet_received) do
            {:ok, peer_msg, rest_buffer} ->
              GenServer.cast(self(), {:received_msg, peer_msg})
              new_state = %{state | raw_packet_received: rest_buffer}
              do_decode_tcp(new_state)
          end
        end
      end
    end
  end

  def on_received_peer_msg(:keepalive, state) do
    # do nothing
    # state.peer_last_active_time already updated in handle_cast/2
    {:noreply, state}
  end
  def on_received_peer_msg({:handshake, info_hash, peer_id, supported_features}, state) do
    # When receving handshake message:
    # 1. Verify torrent info_hash
    # 2. If peer supports BEP10, send then extension-handshake message
    cond do
      # Verify INFO_HASH and PEER_ID
      info_hash !== state.torrent_data.info_hash ->
        {:stop,
         "info_hash not matched: (theirs) #{inspect(info_hash)} vs. (mine) #{
           inspect(state.torrent_data.info_hash)
         }", state}

      # peer_id !== state.peer_id ->
      #     {:stop, "peer_id not matched: (theirs) #{inspect peer_id} vs. (mine) #{inspect state.peer_id}}", state}
      true ->
        if :extension_protocol in supported_features do
          this = self()
          Task.start_link(fn -> GenServer.call(this, :send_extension_handshake) end)
        else
          :noop
        end
        new_state = %{state |
                        received_handshake: true,
                        peer_id: peer_id,
                        peer_support: supported_features,
                      }
        {:noreply, new_state}
    end
  end
  def on_received_peer_msg({:extended, :raw, ext_msg_id, _ext_msg_body}=raw_msg, state) do
    ext_msg = if ext_msg_id == 0 do
      PeerWireProtocol.decode_extend(raw_msg, "handshake")
    else
      {ext_msg_type, ^ext_msg_id} =
        Enum.find(state.peer_extend_msg_mappings, fn {_k, v} -> v == ext_msg_id end)
      PeerWireProtocol.decode_extend(raw_msg, ext_msg_type)
    end
    Logger.debug("Got ext msg #{inspect(ext_msg)}")
    on_received_peer_msg(ext_msg, state)
  end
  def on_received_peer_msg({:extended, "handshake", info}, state) do
    if Map.has_key?(info, "metadata_size") do
      :ok = GenServer.call(state.fileserver_pid, {:metadata_size, info["metadata_size"]})
    else
      :noop
    end
    # TODO: deal with the extension-disabling case
    msg_mappings = Map.get(info, "m", %{})
    new_state = %{state | peer_extend_msg_mappings: msg_mappings}
    {:noreply, new_state}
  end
  def on_received_peer_msg({:extended, "ut_metadata", info}, state) do
    # http://bittorrent.org/beps/bep_0009.html
    Logger.debug("Got ext msg: ut_metadata, info: #{inspect(info)}")
    case info do
      # request-msg
      %{"msg_type" => 0, "piece" => _index} ->
        # TODO: send metadata-piece
        :noop
      # data-msg
      %{"msg_type" => 1, "piece" => index, "total_size" => _total_size, "data" => data} ->
        :ok = GenServer.call(state.fileserver_pid, {:add_metadata_piece, index, data})
      # reject-msg
      %{"msg_type" => 2, "piece" => index} ->
        Logger.warn("Peer does not have metadata piece #{index}")
    end
    {:noreply, state}
  end
  def on_received_peer_msg(:choke, state) do
    new_state = %{state | am_choked: true}
    {:noreply, new_state}
  end
  def on_received_peer_msg(:unchoke, state) do
    new_state = %{state | am_choked: false}
    {:noreply, new_state}
  end
  def on_received_peer_msg(:interested, state) do
    new_state = %{state | am_interested: true}
    {:noreply, new_state}
  end
  def on_received_peer_msg(:not_interested, state) do
    new_state = %{state | am_interested: false}
    {:noreply, new_state}
  end
  def on_received_peer_msg({:have, piece_index}, state) do
    # piece_indicators[i] === 1 means peer have data of piece-index i
    # piece_indicators: [0,1,0,1,0,0,...]   1 means 'have piece'
    new_state = %{state | peer_has_pieces: MapSet.put(state.peer_has_pieces, piece_index)}
    {:noreply, new_state}
  end
  def on_received_peer_msg({:bitfield, bitfield}, state) when is_bitstring(bitfield) do
    vals = bitfield_to_list(bitfield, state.torrent_data.piece_counts)

    have_index_list =
      for {val, idx} <- Enum.with_index(vals), val == 1 do
        idx
      end

    new_state = %{state | peer_has_pieces: MapSet.new(have_index_list)}
    {:noreply, new_state}
  end
  def on_received_peer_msg({:request, piece_index, offset_begin, chunk_length}, state) do
    # Peer requested for a segment of piece data (a.ka. chunk)

    chunk = {piece_index, offset_begin, chunk_length}

    # Add the request to request buffer if its not a duplicated request
    if Enum.member?(state.peer_requested_chunks, chunk) do
      {:noreply, state}
    else
      new_state = %{state | peer_requested_chunks: [chunk | state.peer_requested_chunks]}
      {:noreply, new_state}
    end
  end
  def on_received_peer_msg({:piece, piece_index, offset_begin, chunk_data}, state) do
    # Peer sends us a chunk of piece data.
    # 1. Forward the piece data to FileServer.
    # 2. If the piece data forms a complete piece, send out 'have' msg to notify the client
    req = {:add_piece_chunk, piece_index, offset_begin, chunk_data}
    stats2 = Map.put(state.stats, :received, state.stats.received + byte_size(chunk_data))
    state2 = %{state | stats: stats2}

    case GenServer.call(state2.fileserver_pid, req) do
      :ok ->
        {:noreply, state2}

      {:ok, :have_piece} ->
        {:noreply, state2}
    end
  end
  def on_received_peer_msg({:cancel, piece_index, offset_begin, chunk_length}, state) do
    req = {piece_index, offset_begin, chunk_length}
    new_state = %{state | peer_requested_chunks: List.delete(state.peer_requested_chunks, req)}
    {:noreply, new_state}
  end
  def on_received_peer_msg({:port, _listen_port}, state) do
    # This is optional (for DHT)
    {:noreply, state}
  end
  def on_received_peer_msg(unknown_msg, state) do
    Logger.warn("Ignored unknown peer message #{inspect(unknown_msg)}")
    {:noreply, state}
  end

  def send_binary_data_to_peer(data, socket) when is_binary(data) do
    :gen_tcp.send(socket, data)
    Logger.debug("Sent binary data to peer: #{inspect(data)}")
  end

  def bitfield_to_list(bitfield, piece_counts) when is_binary(bitfield) do
    # Transfers the bitfield message into list of 0/1 elements
    # where element <i> indicates whether the peer holds the data of piece <i>

    {significant, padding} = Enum.split(bitstring_to_list(bitfield), piece_counts)

    Logger.debug(
      "Bitfield(#{bit_size(bitfield)}). Significant(#{length(significant)}). Padding(#{
        length(padding)
      })"
    )

    if not Enum.all?(padding, fn x -> x == 0 end) do
      raise("Non-zero paddings of bitfield: #{inspect(padding)}")
    end

    if Enum.all?(significant, fn x -> x == 1 end) do
      Logger.debug("This is a seeder")
    end

    significant
  end

  def bitstring_to_list(s) do
    # <<0::1, 0::1, 1::1, 0::1>> -> [0, 0, 1, 0]
    Enum.reverse(bitstring_to_reversed_list(s, []))
  end

  def bitstring_to_reversed_list(s, acc) do
    # <<0::1, 0::1, 1::1, 0::1>> -> [0, 1, 0, 0]
    case s do
      <<>> -> acc
      <<x::1, rest::bits>> -> bitstring_to_reversed_list(rest, [x | acc])
    end
  end

  def test_bitstring_to_list() do
    [0, 1, 1, 0, 1, 1, 0, 0] =
      bitstring_to_list(<<0::1, 1::1, 1::1, 0::1, 1::1, 1::1, 0::1, 0::1>>)
  end

  def test_bitfield_to_list() do
    bitfield = <<0::1, 1::1, 1::1, 0::1, 1::1, 1::1, 0::1, 0::1>>
    [0, 1, 1, 0, 1, 1] = bitfield_to_list(bitfield, 6)
  end

  def test_all() do
    test_bitfield_to_list()
    test_bitstring_to_list()
  end
end

defmodule PeerWireProtocol do
  require Logger

  def max_piece_request_length do
    # 30 KB
    30720
  end

  def keepalive_interval_sec do
    20 * 60
  end

  def msg_size_ptr_length do
    # in bytes
    4
  end

  def metadata_piece_length do
    # 16 KiB
    16 * 1024
  end

  def get_supported_features(reserved_bytes) do
    # Parse the reserved bytes of handshake message
    # Returns list of supported feature names
    # https://wiki.theory.org/index.php/BitTorrentSpecification#Official_Extensions_To_The_Protocol
    Logger.debug("Reserved Bytes of handshake: #{inspect(reserved_bytes)}")
    feats = []
    if DHTUtils.get_bit(reserved_bytes, 43) == 1 do
      [:extension_protocol | feats]
    else
      feats
    end
  end

  def msg_type(msg) do
    cond do
      is_atom(msg) -> msg
      is_tuple(msg) -> elem(msg, 0)
    end
  end

  def msg_id(msg) do
    case msg_type(msg) do
      :choke -> 0
      :unchoke -> 1
      :interested -> 2
      :not_interested -> 3
      :have -> 4
      :bitfield -> 5
      :request -> 6
      :piece -> 7
      :cancel -> 8
      :port -> 9
      :extended -> 20
      :handshake -> nil
      :keepalive -> nil
      _ -> raise("Unknown msg type #{inspect(msg)}")
    end
  end

  def encode(:keepalive) do
    <<0::32>>
  end

  def encode(:choke) do
    <<1::32>> <> <<msg_id(:choke)>>
  end

  def encode(:unchoke) do
    <<1::32>> <> <<msg_id(:unchoke)>>
  end

  def encode(:interested) do
    <<1::32>> <> <<msg_id(:interested)>>
  end

  def encode(:not_interested) do
    <<1::32>> <> <<msg_id(:not_interested)>>
  end

  def encode(:have, piece_index) do
    <<5::32, msg_id(:have), piece_index::32>>
  end

  def encode(:bitfield, bitfield) when is_binary(bitfield) do
    # bitfield: <len=0001+X><id=5><bitfield>
    Logger.debug("encoding #{inspect({:bitfield, bitfield})}")
    msg_id(:bitfield)
    msg_body = <<msg_id(:bitfield)>> <> bitfield
    <<byte_size(msg_body)::32>> <> msg_body
  end

  def encode(:port, listen_port) do
    # port: <len=0003><id=9><listen-port>
    <<3::32>> <> <<msg_id(:port), listen_port::bytes-size(2)>>
  end

  def encode(:extended, :handshake, info) do
    # For the extention protocol check
    # http://www.libtorrent.org/extension_protocol.html
    ids = Map.values(Map.get(info, "m", []))

    if ids != Enum.uniq(ids) do
      raise "IDs in msg_mappings cannot be duplicated: #{inspect(ids)}"
    else
      encode(:extended, :raw, 0, Bencoding.encode(info))
    end
  end

  def encode(:extended, :raw, extend_msg_id, raw_data)
      when is_binary(raw_data) and is_integer(extend_msg_id) do
    # For the extention protocol check
    # http://www.libtorrent.org/extension_protocol.html
    msg_body = <<msg_id(:extended), extend_msg_id>> <> raw_data
    <<byte_size(msg_body)::32>> <> msg_body
  end

  def encode(:handshake, info_hash, peer_id, supported_features) do
    # handshake: <pstrlen><pstr><reserved><info_hash><peer_id>
    reserved =
      List.foldl(supported_features, <<0::64>>, fn feat, acc ->
        case feat do
          :extension_protocol ->
            DHTUtils.set_bit(acc, 43, 1)
          unknown ->
            raise "Unknown feature: #{inspect(unknown)}"
        end
      end)

    <<19>> <> "BitTorrent protocol" <> reserved <> info_hash <> peer_id
  end

  def encode(:request, piece_index, begin_offset, block_length) do
    # request: <len=0013><id=6><index><begin><length>
    <<13::32, msg_id(:request), piece_index::32, begin_offset::32, block_length::32>>
  end

  def encode(:piece, piece_index, begin_offset, chunk_data) do
    # piece: <len=0009+X><id=7><index><begin><block of bytes-size X>
    msg_body = <<msg_id(:piece)>> <> <<piece_index::32>> <> <<begin_offset::32>> <> chunk_data
    <<byte_size(msg_body)::32>> <> msg_body
  end

  def encode(:cancel, piece_index, offset_begin, block_length) do
    # cancel: <len=0013><id=8><index><begin><length>
    <<13::32, msg_id(:cancel), piece_index::32, offset_begin::32, block_length::32>>
  end

  def decode!(bin_data) do
    {:ok, decoded_msg, <<"">>} = decode_one(bin_data)
    decoded_msg
  end

  def decode_one(buffer) when is_binary(buffer) do
    # Check if the message PREFIX matches any of the peer-wire-protocol msgs
    # Returns {:ok, decoded_msg, buffer_remain} |　{:error, reason}
    # each peer_msg is represented as a tuple or single atom
    Logger.debug("Decoding buffer #{inspect(buffer)}")

    result =
      case buffer do
        <<19>> <>
            "BitTorrent protocol" <>
            <<reserved::64, info_hash::160, peer_id::160, buffer_remain::binary>> ->
          feats = get_supported_features(<<reserved::64>>)
          decoded_msg = {:handshake, <<info_hash::160>>, <<peer_id::160>>, feats}
          {:ok, decoded_msg, buffer_remain}

        <<0::32, buffer_remain::binary>> ->
          decoded_msg = :keepalive
          {:ok, decoded_msg, buffer_remain}

        <<size_ptr::32, rest::binary>> ->
          num_bits = size_ptr * 8
          <<msg_body::size(num_bits), buffer_remain::binary>> = <<rest::binary>>
          <<msg_id, body::binary>> = <<msg_body::size(num_bits)>>

          decoded_msg =
            cond do
              msg_id == PeerWireProtocol.msg_id(:choke) ->
                :choke

              msg_id == PeerWireProtocol.msg_id(:unchoke) ->
                :unchoke

              msg_id == PeerWireProtocol.msg_id(:interested) ->
                :interested

              msg_id == PeerWireProtocol.msg_id(:not_interested) ->
                :not_interested

              msg_id == PeerWireProtocol.msg_id(:have) ->
                piece_index = :binary.decode_unsigned(body)
                {:have, piece_index}

              msg_id == PeerWireProtocol.msg_id(:bitfield) ->
                bitfield = body
                {:bitfield, bitfield}

              msg_id == PeerWireProtocol.msg_id(:request) ->
                <<piece_index::32, offset_begin::32, chunk_length::32>> = body
                {:request, piece_index, offset_begin, chunk_length}

              msg_id == PeerWireProtocol.msg_id(:piece) ->
                <<piece_index::32, offset_begin::32, chunk_data::binary>> = body
                {:piece, piece_index, offset_begin, chunk_data}

              msg_id == PeerWireProtocol.msg_id(:cancel) ->
                <<piece_index::32, offset_begin::32, chunk_length::32>> = body
                {:cancel, piece_index, offset_begin, chunk_length}

              msg_id == PeerWireProtocol.msg_id(:port) ->
                <<listen_port::16>> = body
                {:port, listen_port}

              msg_id == PeerWireProtocol.msg_id(:extended) ->
                <<ext_msg_id, ext_msg_body::binary>> = body
                {:extended, :raw, ext_msg_id, ext_msg_body}
            end

          {:ok, decoded_msg, buffer_remain}

        _ ->
          {:error, :cannot_decode}
      end

    Logger.debug("Result of decode: #{inspect(result)}")
    result
  end

  def decode_extend(extend_msg, extend_msg_type) do
    # Ignores ext_msg_id since each peer may use different msg-id mappings
    # http://www.bittorrent.org/beps/bep_0010.html
    # TODO: refactor
    {:extended, :raw, ext_msg_id, ext_msg_body} = extend_msg

    case extend_msg_type do
      "handshake" ->
        ^ext_msg_id = 0
        info = Bencoding.decode(ext_msg_body)
        {:extended, "handshake", info}

      "ut_metadata" ->
        info = case Bencoding.decode_prefix(ext_msg_body, %{}) do
          {%{"msg_type" => 0, "piece" => _index}=info, <<"">>} ->
            info
          {%{"msg_type" => 1, "piece" => _index, "total_size" => _total_size}=info, data} ->
            %{info | "data" => data}
          {%{"msg_type" => 2, "piece" => _index}=info, <<"">>} ->
            info
        end
        {:extended, "ut_metadata", info}
    end
  end
end

defmodule JobControl do
  use GenServer
  require Logger

  defstruct [
    my_p2p_id: nil,
    listen_port: nil,
    checking_period: 10 * 1000,
    fileserver_pid: nil,
    peer_discovery_pid: nil,
    connectors: [],
    auto_processing_timer: nil,
    auto_connecting_timer: nil,
    max_connections: 1,
    max_request_chunk_length: 10240,
    fileserver_monitor: nil,
  ]

  def start(init_args) do
    GenServer.start(__MODULE__, init_args, timeout: 60 * 1000)
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, timeout: 60 * 1000)
  end

  def init(fileserver_pid) do

    my_p2p_id = :crypto.strong_rand_bytes(20) |> Base.encode64() |> binary_part(0, 20)
    # TODO: make it configureable
    listen_port = 8999
    dht_port = 6881
    torrent_info_hash = GenServer.call(fileserver_pid, :torrent_info_hash)
    # {:ok, tracker_client_pid} = TrackerClient.start_link([my_p2p_id, listen_port, fileserver_pid])
    {:ok, peer_discovery_pid} = PeerDiscovery.start_link([my_p2p_id, torrent_info_hash, dht_port])
      state = %JobControl{
      my_p2p_id: my_p2p_id,
      listen_port: listen_port,
      checking_period: 10 * 1000,
      fileserver_pid: fileserver_pid,
      peer_discovery_pid: peer_discovery_pid,
      connectors: [],
      auto_processing_timer: nil,
      auto_connecting_timer: nil,
      max_connections: 1,
      max_request_chunk_length: 10240,
      fileserver_monitor: Process.monitor(fileserver_pid),
    }

    GenServer.cast(self(), :connect)
    Process.flag(:trap_exit, true)
    Logger.info("JobControl Initialized: #{inspect(state)}")
    {:ok, state}
  end

  def terminate(reason, _state) do
    Logger.info("JobControl terminated: #{inspect(reason)}")
  end

  def handle_call(:state, _from, state) do
    # For debugging
    {:reply, state, state}
  end

  def handle_call(:get_fileserver, _from, state) do
    {:reply, state.fileserver_pid, state}
  end

  def handle_call(:get_tracker_client, _from, state) do
    {:reply, state.tracker_client_pid, state}
  end

  def handle_call(:download_progress, _from, state) do
    info = GenServer.call(state.fileserver_pid, :get_piece_info)

    response = %{
      finished: length(info.finished),
      unfinished: length(info.unfinished)
    }

    {:reply, response, state}
  end

  def handle_call({:auto_processing, is_enabled}, _from, state) do
    cond do
      is_enabled == true and state.auto_processing_timer == nil ->
        ref = Process.send_after(self(), :make_decision, state.checking_period)
        Logger.info("[Control] auto_processing enabled")
        new_state = %{state | auto_processing_timer: ref}
        {:reply, :ok, new_state}

      is_enabled == false and state.auto_processing_timer != nil ->
        r = Process.cancel_timer(state.auto_processing_timer)
        Logger.info("[Control] auto_processing disabled")
        new_state = %{state | auto_processing_timer: nil}
        {:reply, r, new_state}

      is_enabled == true and state.auto_processing_timer != nil ->
        Logger.info("[Control] auto_processing_timer: already enabled")
        {:reply, :ok, state}

      is_enabled == false and state.auto_processing_timer == nil ->
        Logger.info("[Control] auto_processing_timer: already disabled")
        {:reply, :ok, state}
    end
  end

  def handle_call({:auto_processing_interval, n}, _from, state) do
    new_state = %{state | checking_period: n}
    {:reply, :ok, new_state}
  end

  def handle_call({:auto_connecting, is_enabled}, _from, state) do
    cond do
      is_enabled == true and state.auto_connecting_timer == nil ->
        ref = Process.send_after(self(), :auto_connect_loop, state.checking_period)
        Logger.info("[Control] auto_connecting enabled")
        new_state = %{state | auto_connecting_timer: ref}
        {:reply, :ok, new_state}

      is_enabled == false and state.auto_connecting_timer != nil ->
        r = Process.cancel_timer(state.auto_connecting_timer)
        Logger.info("[Control] auto_connecting disabled")
        new_state = %{state | auto_connecting_timer: nil}
        {:reply, r, new_state}

      is_enabled == true and state.auto_connecting_timer != nil ->
        Logger.info("[Control] auto_connecting: already enabled")
        {:reply, :ok, state}

      is_enabled == false and state.auto_connecting_timer == nil ->
        Logger.info("[Control] auto_connecting: already disabled")
        {:reply, :ok, state}
    end
  end

  def handle_call({:max_connections, num}, _from, state) do
    num_reduntant = length(state.connectors) - num

    if num_reduntant > 0 do
      cons = Enum.take_random(state.connectors, num_reduntant)
      Logger.info("Closing #{num_reduntant} reduntant connections")

      tasks =
        Task.async_stream(
          cons,
          fn x ->
            Logger.info("Closing connection #{inspect(x)}...")
            GenServer.stop(x, :normal)
          end
        )

      Enum.to_list(tasks)
    end

    {:reply, :ok, %{state | max_connections: num}}
  end

  def handle_call(:get_connections, _from, state) do
    {:reply, state.connectors, state}
  end

  def handle_call(:close_connections, _from, state) do
    tasks =
      Task.async_stream(
        state.connectors,
        fn x ->
          Logger.info("Closing connection #{inspect(x)}...")
          GenServer.stop(x, :normal)
        end,
        timeout: 10000
      )

    results = Enum.to_list(tasks)
    {:reply, results, state}
  end

  def handle_call(unknown_msg, _from, state) do
    Logger.error("Unknown msg for handle_call: #{inspect(unknown_msg)}")
    {:reply, {:error, :unknown_msg}, state}
  end

  def handle_cast(:connect, state) do
    # Randomly connect to one of the peer listed in the tracker response
    # ONLY IF current number of connections does not reach the limit defined in state
    quota = state.max_connections - length(state.connectors)

    if quota <= 0 do
      {:noreply, state}
    else
      found_peers = GenServer.call(state.peer_discovery_pid, :found_peers)
      Logger.debug("Found peers: #{inspect(found_peers)}")
      tasks =
        Task.async_stream(
          state.connectors,
          fn x ->
            try do
              peer = GenServer.call(x, :peer_info)
              {peer.ip, peer.port}
            catch
              :exit, _e -> nil
            end
          end,
          timeout: 2000,
          on_timeout: :kill_task
        )

      peer_conns = Enum.reject(tasks, fn x -> x == nil or x == {:exit, :timeout} end)

      peer_candidates =
        Enum.filter(
          found_peers,
          fn x ->
            cond do
              {x["ip"], x["port"]} in peer_conns -> false
              true -> true
            end
          end
        )

      peers = Enum.take_random(peer_candidates, quota)
      Logger.debug("Randomly picked peers #{inspect(peers)}")

      Enum.map(
        peers,
        fn x ->
          GenServer.cast(self(), {:connect, x["ip"], x["port"]})
        end
      )

      {:noreply, state}
    end
  end

  def handle_cast({:connect, peer_ip, peer_port}, state) do
    # Connect to destination IP:Port
    # This does not care about the max-connection limit defined in state

    peer_id = nil
    torrent_data = GenServer.call(state.fileserver_pid, :torrent_data)

    # Convert IP: "1.2.3.4" -> {1,2,3,4}
    peer_ip_addr = case peer_ip do
      x when is_tuple(x) ->
        x
      x when is_binary(x) ->
        {:ok, addr} = :inet.getaddr(String.to_charlist(x) , :inet)
        addr
    end

    args = [peer_ip_addr, peer_port, torrent_data, state.my_p2p_id, peer_id, state.fileserver_pid]
    this = self()
    spawn_link(fn ->
                 case Connector.start(args) do
                    {:ok, pid} ->
                      send(this, {:connected, pid})
                    {:error, _reason} ->
                      :do_nothing
                 end
               end)
    {:noreply, state}
  end

  def handle_cast(:make_decision, state) do
    send(self(), :make_decision)
    {:noreply, state}
  end

  def handle_cast({:policy, :max_request_chunk_length, num}, state) do
    {:noreply, %{state | max_request_chunk_length: num}}
  end

  def handle_cast(unknown_msg, state) do
    Logger.error("[Control] handle_cast received unknown msg: #{inspect(unknown_msg)}")
    {:noreply, state}
  end

  def handle_info({:connected, connector_pid}, state) do
    Logger.debug("[Control] connected: #{inspect(connector_pid)}")
    Process.link(connector_pid)
    new_state = %{state | connectors: [connector_pid|state.connectors]}
    {:noreply, new_state}
  end
  def handle_info({:incoming_connect, socket}, state) do
    init_args = [socket, state.torrent_data, state.my_p2p_id, nil, state.fileserver_pid]
    {:ok, pid} = Connector.start_link(init_args)
    :ok = :gen_tcp.controlling_process(socket, pid)
    con_list = [pid | state.connectors]
    {:noreply, %{state | connectors: con_list}}
  end
  def handle_info({:EXIT, pid, reason}, state) do
    cond do
      Enum.member?(state.connectors, pid) ->
        Logger.debug("Connector #{inspect(pid)} died (#{inspect(reason)}). Remove PID ref")
        {:noreply, %{state | connectors: List.delete(state.connectors, pid)}}

      pid == state.fileserver_pid ->
        {:stop, {"fileserver terminated", reason}, state}

      true ->
        case reason do
          :normal ->
            Logger.debug("Process #{inspect(pid)} died: #{inspect(reason)}")

          _ ->
            Logger.error("Process #{inspect(pid)} died: #{inspect(reason)}")
        end

        {:noreply, state}
    end
  end
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    if ref === state.fileserver_monitor do
      Logger.info("fileserver down. exiting...")
      {:stop, :fileserver_down, state}
    else
      Logger.warn("Process DOWN: pid: #{inspect(pid)}, reason: #{inspect(reason)}")
      {:noreply, state}
    end
  end
  def handle_info(:make_decision, state) do
    # TODO: refactor this
    decoded_torrent = GenServer.call(state.fileserver_pid, :decoded_torrent)
    metadata_piece_cache = GenServer.call(state.fileserver_pid, :metadata_piece_cache)
    state2 = cond do
      decoded_torrent != nil ->
        do_download_piece(state)
      decoded_torrent == nil and metadata_piece_cache != nil ->
        do_download_metadata(state)
      true ->
        Logger.debug("[make_decision] Neither torrent nor metadata_cache has been initialized. Skip this round.")
        state
    end

    # Set the timer for the next round (if enabled)
    case state.auto_processing_timer do
      nil ->
        {:noreply, state2}
      timer ->
        Process.cancel_timer(timer)
        new_timer = Process.send_after(self(), :make_decision, state2.checking_period)
        {:noreply, %{state2 | auto_processing_timer: new_timer}}
    end
  end
  def handle_info(:auto_connect_loop, state) do
    if state.auto_connecting_timer != nil and state.max_connections > length(state.connectors) do
      n = state.max_connections - length(state.connectors)
      Enum.map(1..n, fn _x -> GenServer.cast(self(), :connect) end)
    end

    if state.auto_connecting_timer != nil do
      Process.cancel_timer(state.auto_connecting_timer)
      new_timer = Process.send_after(self(), :auto_connect_loop, state.checking_period)
      {:noreply, %{state | auto_connecting_timer: new_timer}}
    else
      {:noreply, state}
    end
  end
  def handle_info({:have_piece, piece_index}, state) do
    # The event `:have_piece` means we have this piece on disk
    tasks =
      for pid <- state.connectors do
        Task.async(fn ->
          try do
            GenServer.call(pid, {:send_msg, {:have, piece_index}})
          catch
            :exit, reason -> {:error, reason}
          end
        end)
      end

    Enum.map(tasks, fn x -> Task.await(x, :infinity) end)
    {:noreply, state}
  end
  def handle_info(unknown_msg, state) do
    Logger.error("[Control] unknown msg to handle_info/2: #{inspect(unknown_msg)}")
    {:noreply, state}
  end

  # =================================================================
  #                   Util-functions
  # =================================================================
  def do_download_metadata(state) do
    # Tell the connectors to request metadata-pieces from their connected peers
    indices = GenServer.call(state.fileserver_pid, :missing_metadata_pieces)
    {_, tasks} = List.foldl(state.connectors, {indices, []},
      fn (pid, {idx_list, tasks}=acc) ->
        cond do
          idx_list == [] ->
            acc
          :extension_protocol not in :sys.get_state(pid).peer_support ->
            GenServer.stop(pid, :normal)
            acc
          true ->
            [idx|rest] = idx_list
            t = Task.async(fn -> GenServer.call(pid, {:request_metadata_piece, idx}) end)
            {rest, [t|tasks]}
        end
      end)
    Enum.each(tasks, fn t -> Task.await(t) end)
    state
  end

  def do_download_piece(state) do
    # Tell the connectors to request pieces from their connected peers
    if state.connectors == [] do
      Logger.debug("[Controller] make_decision: No connected peer. Skip this round")
    else
      # ==== Serve 1 request for each connection ====
      stream =
        Task.async_stream(
          state.connectors,
          &cmd_serve_one_chunk/1,
          timeout: 10000,
          on_timeout: :kill_task
        )

      Enum.to_list(stream)

      # Collect piece info from each peer
      they_have_pieces =
        for pid <- state.connectors, into: %{} do
          have_pieces =
            try do
              GenServer.call(pid, :peer_has_pieces)
            catch
              :exit, _reason -> []
              _error -> []
            end

          {pid, have_pieces}
        end

      # Map piece-requets to connected peers
      i_need_pieces = GenServer.call(state.fileserver_pid, :get_unfinished_pieces)
      request_mapping = plan_download_request(i_need_pieces, they_have_pieces)
      Logger.debug("request_mapping: #{inspect(request_mapping)}")
      # Tell connectors to send out piece-requests
      tasks =
        for {piece_index, connector_pid} <- request_mapping do
          Task.async(fn ->
            cmd_request_one_chunk(
              state.fileserver_pid,
              connector_pid,
              piece_index,
              state.max_request_chunk_length
            )
          end)
        end

      Enum.map(tasks, fn x -> Task.await(x, :infinity) end)
    end
    state
  end

  def plan_download_request(i_need_pieces, they_have_pieces) do
    # i_need_pieces: list of piece_index
    # they_have_pieces: list of {pid, index_list}
    # Spread our piece-requests to the connected peers
    # Returns:  [{piece_index, pid_for_requesting_this_piece}, ...]

    # resource_map = [{<piece_index>, <PIDs that have this piece>}, ...]

    resource_map =
      for piece_index <- i_need_pieces do
        pid_list =
          for {pid, index_list} <- they_have_pieces, Enum.member?(index_list, piece_index) do
            pid
          end

        {piece_index, pid_list}
      end

    sorted_resource_map =
      Enum.sort_by(
        resource_map,
        fn {_i, pid_list} -> length(pid_list) end
      )

    max_requests = Enum.count(they_have_pieces)

    {planned_requests, _} =
      Enum.reduce_while(sorted_resource_map, {[], []}, fn {piece_index, pid_list} = _x,
                                                          {requests, used_pids} = acc ->
        if Enum.count(used_pids) == max_requests do
          {:halt, acc}
        else
          case pid_list -- used_pids do
            [] ->
              {:cont, acc}

            [pid | _] ->
              acc2 = {[{piece_index, pid} | requests], [pid | used_pids]}
              {:cont, acc2}
          end
        end
      end)

    planned_requests
  end

  # def plan(i_need_pieces, they_have_pieces) do
  #     pids = Enum.map(they_have_pieces, fn {pid, _index_list} -> pid end)
  #     piece_and_owners = for piece_index <- i_need_pieces do
  #         pid_list = for {pid, index_list} <- they_have_pieces, Enum.member?(index_list, piece_index) do
  #             pid
  #         end
  #         {piece_index, pid_list}
  #     end
  #
  #     # Rarest piece first
  #     sorted = Enum.sort_by(resource_map,
  #                          fn {_i, pid_list} ->  length(pid_list) end)
  #     Enum.reduce_while(sorted, {[], pids},
  #         fn ({piece_idx, pids}, {requests, free_pids}=acc) ->
  #             if free_pids == [] do
  #                 {:halt, acc}
  #             else
  #                 # intersect of free_pids and pids
  #                 p_list = free_pids -- (free_pids -- pids)
  #                 Enum.reduce_while(p_list, acc, fun)
  #             end
  #         end)
  # end

  def cmd_serve_one_chunk(connector_pid) do
    # Tell connector to response to the peer's request, send 1 piece chunk to peer.
    # returns on of the folowing:
    #   {:ok, :no_request} - peer did not request any chunk
    #   response_from_connector
    #   {:error, reason}
    try do
      peer_requested_chunks = GenServer.call(connector_pid, :peer_requested_chunks)
      Logger.debug("peer requested chunks: #{inspect(peer_requested_chunks)}")
      # Process 1 peer request
      case peer_requested_chunks do
        [] ->
          :no_request

        [chunk | _] ->
          {piece_index, offset_begin, chunk_length} = chunk

          GenServer.call(
            connector_pid,
            {:send_chunk, piece_index, offset_begin, chunk_length}
          )
      end
    catch
      :exit, {:timeout, info} ->
        Logger.warn("timeout: cmd_serve_one_chunk of #{inspect(connector_pid)}: #{inspect(info)}")
        {:error, :timeout}

      :exit, reason ->
        Logger.error("#{inspect(connector_pid)} :exit, #{inspect(reason)}")

      e ->
        Logger.error("catch: #{inspect(e)}")
        {:error, e}
    end
  end

  def cmd_request_one_chunk(fileserver_pid, connector_pid, piece_index, max_request_length) do
    # Tell the connector to request the fisrt missing chunk of the given piece_index

    case GenServer.call(fileserver_pid, {:get_required_chunks, piece_index}) do
      [] ->
        :do_nothing

      [{offset_start, offset_end} | _] ->
        chunk_length = offset_end - offset_start + 1
        data_length = min(chunk_length, max_request_length)
        req = {:request_piece, piece_index, offset_start, data_length}

        try do
          case GenServer.call(connector_pid, req) do
            :ok ->
              :ok

            {:error, :i_am_choked} ->
              {:skipped, :i_am_choked}

            err ->
              Logger.info("Error: #{inspect(err)}")
              {:error, err}
          end
        catch
          :exit, reason -> {:error, {:exit, reason}}
          err -> {:error, err}
        end
    end
  end
end

defmodule Listener do
  require Logger

  def start(listen_port, job_control_pid) do
    Logger.info("Listening on port #{listen_port}")
    listen_socket = :gen_tcp.listen(listen_port, [:binary, active: false])
    loop(listen_socket, job_control_pid)
  end

  def loop(listen_socket, job_control_pid) do
    {:ok, socket} = :gen_tcp.accept(listen_socket)
    {:ok, info} = :inet.peername(socket)
    Logger.info("Accepted incoming connection #{inspect(socket)} from #{inspect(info)}")
    :ok = :gen_tcp.controlling_process(socket, job_control_pid)
    send(job_control_pid, {:incoming_connect, socket})
    loop(listen_socket, job_control_pid)
  end
end

defmodule TrackerClient do
  use GenServer
  require Logger

  @default_update_interval_sec 60

  def start(init_args) do
    GenServer.start(__MODULE__, init_args)
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  def init([my_p2p_id, my_listen_port, fileserver_pid]) do
    torrent_data = GenServer.call(fileserver_pid, :torrent_data)
    init([my_p2p_id, my_listen_port, fileserver_pid, torrent_data.tracker_url])
  end

  def init([my_p2p_id, my_listen_port, fileserver_pid, tracker_url]) do
    state = %{
      my_p2p_id: my_p2p_id,
      my_p2p_port: my_listen_port,
      tracker_url: tracker_url,
      fileserver_pid: fileserver_pid,
      update_timer: Process.send_after(self(), :update_loop, 0),
      peer_network_status: %{"peers" => []}
    }

    {:ok, state}
  end

  def handle_call(:state, _from, state) do
    # For debugging
    {:reply, state, state}
  end

  def handle_call({:auto_update, activate}, _from, state) do
    case {state.update_timer, activate} do
      {nil, false} ->
        {:reply, :ok, state}

      {timer, false} ->
        Process.cancel_timer(timer)
        new_state = %{state | update_timer: nil}
        {:reply, :ok, new_state}

      {nil, true} ->
        r = GenServer.cast(self(), :trigger_update)
        {:reply, r, state}

      {_timer, true} ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:set_tracker_url, url}, _from, state) do
    new_state = %{state | tracker_url: url}
    {:reply, :ok, new_state}
  end

  def handle_call(:peer_network_status, _from, state) do
    {:reply, state.peer_network_status, state}
  end

  def handle_cast(:trigger_update, state) do
    send(self(), :update_loop)
    {:noreply, state}
  end

  def handle_info(:update_loop, state) do
    # Obtains info from Tracker periodically
    if state.update_timer do
      Process.cancel_timer(state.update_timer)
    end

    my_info = %{
      torrent_info_hash: GenServer.call(state.fileserver_pid, :torrent_info_hash),
      my_p2p_id: state.my_p2p_id,
      my_p2p_port: state.my_p2p_port,
      left: GenServer.call(state.fileserver_pid, :size_left)
    }

    me = self()

    spawn_link(fn ->
      res = exchange_info(state.tracker_url, my_info)
      send(me, {:tracker_response, res})
    end)

    {:noreply, state}
  end

  def handle_info({:tracker_response, result}, state) do
    case result do
      {:ok, resp} ->
        interval = 1000 * Map.get(resp, "interval", @default_update_interval_sec)
        new_timer = Process.send_after(self(), :update_loop, interval)
        {:noreply, %{state | update_timer: new_timer, peer_network_status: resp}}

      {:error, reason} ->
        Logger.warn("Failed to communicate with tracker: #{inspect(reason)}. Retrying...")
        new_timer = Process.send_after(self(), :update_loop, 5)
        {:noreply, %{state | update_timer: new_timer}}
    end
  end

  def exchange_info(tracker_url, info) do
    # Exchange information with Tracker server
    # -> {:ok, peer_network_status} | {:error, reason}
    required_fields = [:torrent_info_hash, :my_p2p_id, :my_p2p_port, :left]

    for x <- required_fields do
      if not Map.has_key?(info, x) do
        raise "Missing required key: #{inspect(x)} got #{inspect(info)}"
      end
    end

    params = [
      {"info_hash", info.torrent_info_hash},
      {"peer_id", info.my_p2p_id},
      {"port", Integer.to_string(info.my_p2p_port)},
      {"uploaded", Integer.to_string(Map.get(info, :uploaded, 0))},
      {"downloaded", Integer.to_string(Map.get(info, :downloaded, 0))},
      # always use compact mode. for compatibility
      {"compact", "1"},
      {"left", Integer.to_string(info.left)}
    ]

    case http_request(tracker_url, params) do
      {:ok, resp} ->
        peer_network_status = parse_tracker_response(resp)
        {:ok, peer_network_status}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def parse_tracker_response(bin_data) when is_binary(bin_data) do
    # Parse raw http response into map-structured data
    # Ref: https://wiki.theory.org/index.php/BitTorrent_Tracker_Protocol
    data = Bencoding.decode(bin_data)
    peer_list = parse_tracker_response_peer_string(data["peers"])
    res = Map.replace!(data, "peers", peer_list)
    Logger.debug("Parsed tracker response: #{inspect(res)}")
    res
  end

  def parse_tracker_response_peer_string(s, parsed_peers \\ []) do
    # -> list of %{"ip" => <ip>, "port" => <port>}
    # Used to parse the compact-form of peer-list
    # http://www.bittorrent.org/beps/bep_0023.html
    case s do
      <<"">> ->
        parsed_peers

      <<a, b, c, d, port::16, rest::binary>> ->
        # a,b,c,d are IP ocets
        ip = Enum.map_join([a, b, c, d], ".", &Integer.to_string/1)
        acc = [%{"ip" => ip, "port" => port} | parsed_peers]
        parse_tracker_response_peer_string(rest, acc)
    end
  end

  def http_request(tracker_url, query_params, timeout \\ 20) do
    # ->   {:ok, raw_response}
    #    | {:error, {curl_exit_code, error_reason}}
    full_url =
      case URI.encode_query(query_params) do
        "" -> tracker_url
        query_string -> tracker_url <> "?" <> query_string
      end

    # Use curl since the erlang :httpc.request/1 does not accept percent-encoded binary data in query string
    # https://www.cyberciti.biz/faq/curl-hide-progress-bar-output-linux-unix-macos/
    # {output, 0} = System.cmd("curl", ["-sSL", "--output", "/tmp/curl_output.bin", full_url])

    Logger.info("Connecting to tracker: #{full_url}")

    case System.cmd("curl", ["--max-time", "#{timeout}", "-sSL", full_url]) do
      {output, 0} ->
        Logger.debug("tracker response: #{inspect(output)}")
        # File.write!("/tmp/torrent_tracker.out", output)
        {:ok, output}

      {error, exit_code} ->
        {:error, {:curl_error, exit_code, error}}
    end
  end
end

defmodule Job do
  def test_all() do
  end

  def test() do

  end

  def check_requirements() do
    try do
      System.cmd("curl", ["--version"])
    catch
      :error, :enoent ->
        raise("[ERROR] Must have CURL installed")
    end
  end

  def create(job_dir, torrent_or_magnet) do
    {:ok, fs_pid} = FileServer.start_link({job_dir, torrent_or_magnet})
    {:ok, control_pid} = JobControl.start_link(fs_pid)
    GenServer.call(fs_pid, {:set_event_manager, control_pid})
    control_pid
  end

  def load(job_dir) do
    {:ok, fs_pid} = FileServer.start_link(job_dir)
    {:ok, control_pid} = JobControl.start_link(fs_pid)
    GenServer.call(fs_pid, {:set_event_manager, control_pid})
    control_pid
  end

  def start(job_control) do
    GenServer.call(job_control, {:auto_processing, true})
    GenServer.call(job_control, {:auto_connecting, true})
  end

  def finished?(job_control) do
    stat = GenServer.call(job_control, :download_progress)
    stat.unfinished == 0
  end

  def progress(job_control) do
    stat = GenServer.call(job_control, :download_progress)
    percentage = 100 * stat.finished / (stat.finished + stat.unfinished)
    Map.put(stat, :percentage, percentage)
  end

  def pause(job_control) do
    # Stop doing dowload/upload actions but maintain the connections
    GenServer.call(job_control, {:auto_connecting, false})
    GenServer.call(job_control, {:auto_processing, false})
  end

  def resume(job_control) do
    GenServer.call(job_control, {:auto_processing, true})
    GenServer.call(job_control, {:auto_connecting, true})
  end

  def stop(job_control) do
    # Stop all kinds of actions
    pause(job_control)
    GenServer.call(job_control, :close_connections)
  end

  def set_max_connections(job_control, n) do
    GenServer.call(job_control, {:max_connections, n})
  end

  def files(job_control) do
    fileserver = GenServer.call(job_control, :get_fileserver)
    GenServer.call(fileserver, :torrent_data).files
  end

  def dir(job_control) do
    fileserver = GenServer.call(job_control, :get_fileserver)
    GenServer.call(fileserver, :base_dir)
  end

  def status(job_control) do
    fileserver = GenServer.call(job_control, :get_fileserver)

    %{
      job_dir: GenServer.call(fileserver, :base_dir),
      progress: progress(job_control),
      connections: length(GenServer.call(job_control, :get_connections)),
      files: files(job_control)
    }
  end
end

defmodule DHTBucket do
  require Logger

  defstruct [
    :capacity,
    :nodes,
    :last_changed,
    :candidates
  ]

  def new_bucket(capacity) do
    # Create an empty bucket
    %DHTBucket{
      capacity: capacity,
      last_changed: System.system_time(:second),
      nodes: [],
      candidates: []
    }
  end

  def add_node(bucket, node) do
    # Add node to bucket if its not in nodes or candidates
    # -> {:ok, bucket} or {:error, :bucket_full}
    cond do
      has_node?(bucket, node.id) ->
        updated_bucket = refresh_node(bucket, node.id)
        {:ok, updated_bucket}

      has_candidate?(bucket, node.id) ->
        {:ok, bucket}

      is_full?(bucket) ->
        {:error, :bucket_full}

      true ->
        updated_nodes = insert_to_sorted_nodes(node, bucket.nodes)
        updated_bucket = %{bucket | nodes: updated_nodes}
        {:ok, updated_bucket}
    end
  end

  def add_candidate(bucket, node) do
    Logger.debug("Add candidate #{inspect(node)} to bucket #{inspect(bucket)}")
    cands = List.insert_at(bucket.candidates, -1, node)
    %{bucket | candidates: cands}
  end

  def delete_node(bucket, node_id) do
    # delete node, also add one from candidate
    buck = without_node(bucket, node_id)

    case buck.candidates do
      [] ->
        buck

      [x | _] ->
        {:ok, buck2} = DHTBucket.add_node(buck, x)
        buck2
    end
  end

  def get_node_by_id(bucket, node_id) do
    # -> Node or nil
    Enum.find(bucket.nodes, fn x -> x.id == node_id end)
  end

  def get_nodes(bucket) do
    bucket.nodes
  end

  def get_n_nodes(bucket, n) do
    Enum.take(bucket.nodes, n)
  end

  def has_node?(bucket, node_id) do
    if get_node_by_id(bucket, node_id) == nil do
      false
    else
      true
    end
  end

  def has_candidate?(bucket, node_id) do
    a = Enum.find(bucket.candidates, fn x -> x.id == node_id end)

    if a == nil do
      false
    else
      true
    end
  end

  def without_node(bucket, node_id) do
    new_nodes = Enum.reject(bucket.nodes, fn x -> x.id == node_id end)
    %{bucket | nodes: new_nodes}
  end

  def get_most_trusty_node(bucket) do
    # -> {node_id, node_data} or nil
    get_trusty_nodes(bucket) |> List.first()
  end

  def get_trusty_nodes(bucket) do
    # -> list of {node_id, node_data}
    now = System.os_time(:second)

    Enum.filter(
      bucket.nodes,
      fn x ->
        now - x.last_active <= healthcheck_threshold()
      end
    )
  end

  def get_most_questionable_node(bucket) do
    # -> {node_id, node_data} or nil
    get_questionable_nodes(bucket) |> List.last()
  end

  def get_questionable_nodes(bucket) do
    # -> list of {node_id, node_data}
    now = System.os_time(:second)

    Enum.filter(
      bucket.nodes,
      fn x ->
        now - x.last_active > healthcheck_threshold()
      end
    )
  end

  def size(bucket) do
    {current_size, _max_size} = size_info(bucket)
    current_size
  end

  def size_info(bucket) do
    current_size = length(bucket.nodes)
    max_size = bucket.capacity
    {current_size, max_size}
  end

  def is_full?(bucket) do
    {current_size, max_size} = size_info(bucket)
    current_size == max_size
  end

  def is_fresh?(bucket) do
    System.system_time(:second) - bucket.last_changed <= 60 * 5
  end

  def refresh_node(bucket, node_id) do
    idx = Enum.find_index(bucket.nodes, fn x -> x.id == node_id end)
    {node, nodes} = List.pop_at(bucket.nodes, idx)
    refreshed_node = %{node | last_active: System.system_time(:second)}
    %{bucket | nodes: insert_to_sorted_nodes(refreshed_node, nodes)}
  end

  def insert_to_sorted_nodes(new_node, sorted_nodes) do
    # returned a list of sorted-nodes,
    # which first element has the most recent active_time
    case sorted_nodes do
      [] ->
        [new_node]

      [x | rest] ->
        if new_node.last_active > x.last_active do
          [new_node | sorted_nodes]
        else
          [x | insert_to_sorted_nodes(new_node, rest)]
        end
    end
  end

  def healthcheck_threshold() do
    # 15 minutes
    15 * 60
  end
end

defmodule DHTTable do
  require Logger

  defstruct buckets: [],
            reference_node_id: nil

  def new_table(reference_node_id, bucket_capacity \\ 20) when is_bitstring(reference_node_id) do
    # Use the bit_size of node_id to determine the space of table
    num_of_buckets = bit_size(reference_node_id)

    buckets =
      for _ <- 0..num_of_buckets do
        DHTBucket.new_bucket(bucket_capacity)
      end

    %DHTTable{
      buckets: buckets,
      reference_node_id: reference_node_id
    }
  end

  def get_node(table, node_id) do
    bucket = get_corresponding_bucket(table, node_id)
    Logger.warn("get_node:  #{inspect(node_id)} try bucket #{inspect(bucket)}")
    DHTBucket.get_node_by_id(bucket, node_id)
  end

  def get_nodes(table) do
    Enum.map(table.buckets, fn b -> b.nodes end) |> List.flatten()
  end

  def reference_node_id(table) do
    table.reference_node_id
  end

  def get_k_nearest_nodes(table, node_id, k \\ 8) do
    # -> [<node>, ...]  sorted by the XOR-distance to the <node_id>
    Enum.sort_by(get_nodes(table),
                  fn node ->
                    DHTUtils.node_distance_binary(node.id, node_id)
                  end)
      |> Enum.take(k)
  end

  def get_bucket_by_distance(table, distance) when is_bitstring(distance) do
    i = get_bucket_num_by_distance(distance)
    Enum.fetch!(table.buckets, i)
  end

  def get_bucket_num_by_distance(distance) when is_bitstring(distance) do
    prefix_len = DHTUtils.count_0_prefix_length(distance)
    # 0101 -> prefix_len 1 -> bucket (4-1)=3
    # 0001 -> prefix_len 3 -> bucket (4-3)=1
    # 0000 -> prefix_len 4 -> bucket (4-4)=0 (reference_node)
    bucket_id = bit_size(distance) - prefix_len
    bucket_id
  end

  def get_corresponding_bucket(table, node_id) do
    distance = DHTUtils.node_distance_binary(table.reference_node_id, node_id)
    get_bucket_by_distance(table, distance)
  end

  def get_corresopnding_bucket_num(table, node_id) do
    dist = DHTUtilds.node_distance_binary(table.reference_node_id, node_id)
    get_bucket_num_by_distance(dist)
  end

  def size(table) do
    # Return number of buckets
    length(table.buckets)
  end
end

defmodule DHTUtils do
  require Logger

  def count_0_prefix_length(n) when is_bitstring(n) do
    count_0_prefix_length(n, 0)
  end

  defp count_0_prefix_length(n, acc) when is_bitstring(n) do
    case n do
      <<>> -> acc
      <<1::1, _rest::bitstring>> -> acc
      <<0::1, rest::bitstring>> -> count_0_prefix_length(rest, acc + 1)
    end
  end

  def node_distance_binary(node_id_1, node_id_2) do
    # Calculate the node distance with XOR and return in binary format
    :crypto.exor(node_id_1, node_id_2)
  end

  def gen_random_bitstring(number_of_bits) do
    # Generates a length-n randomized bitstring
    num_bytes = :erlang.ceil(number_of_bits / 8)
    rand_bytes = :crypto.strong_rand_bytes(num_bytes)
    <<data::size(number_of_bits)-bits, _::bits>> = rand_bytes
    data
  end

  def gen_random_string(string_length) do
    gen_random_bitstring(8 * string_length)
    |> Base.url_encode64()
    |> binary_part(0, string_length)
  end

  def gen_node_id() do
    gen_random_string(20)
  end

  def parse_peers_val(vals) when is_list(vals) do
    # -> [peer1, peer2, ...] where <peer> = %{"ip" => ip_tuple, "port" => port_num}
    Enum.map(vals,
              fn x ->
                <<a, b, c, d, port::16>> = x
                %{"ip" => {a, b, c, d}, "port" => port}
              end)
  end

  def parse_nodes_val(bin) when is_binary(bin) do
    case bin do
      <<"">> ->
        []

      <<x::208-bits, rest::binary>> ->
        node = DHTNode.from_compact_info(x)
        [node | parse_nodes_val(rest)]
    end
  end

  def get_magnet_info_hash(url) do
    # -> {:ok, <info_hash>} or {:error, reason}
    r = URI.parse(url)

    if r.scheme != "magnet" do
      {:error, :invalid_magnet_schema}
    else
      params = URI.query_decoder(r.query) |> Enum.to_list()
      xt_pair = List.keyfind(params, "xt", 0)

      case xt_pair do
        {"xt", "urn:btih:" <> hex_encoded_info_hash} ->
          case Base.decode16(hex_encoded_info_hash, case: :mixed) do
            :error ->
              {:error, :invalid_info_hash}

            {:ok, info_hash} ->
              {:ok, info_hash}
          end

        xt ->
          {:error, {:invalid_xt, xt}}
      end
    end
  end

  def get_bit(bin, index) do
    # Get the bit value at <index>. (Index starts from 0).
    <<_left::size(index), val::1, _right::bits>> = bin
    val
  end

  def set_bit(bin, index, value) do
    # Set the bit value at <index>. (Index starts from 0).
    if value not in [1, 0] do
      raise "Invalid value: #{inspect(value)}"
    end

    <<left::size(index), _::1, right::bits>> = bin
    <<left::size(index), value::1, right::bits>>
  end
end

defmodule DHTNode do
  defstruct [
    :id,
    :ip,
    :port,
    :last_active
  ]

  def from_compact_info(bin) when is_binary(bin) do
    <<node_id::160-bits, a, b, c, d, port::16>> = bin
    ip = {a, b, c, d}
    %DHTNode{id: node_id, ip: ip, port: port}
  end

  def compact_info(node) do
    node.id <> contact_info(node)
  end

  def contact_info(node) do
    {a, b, c, d} = node.ip
    <<a, b, c, d>> <> <<node.port::integer-size(16)>>
  end
end

defmodule KRPC_Msg do
  require Logger
  # http://www.bittorrent.org/beps/bep_0005.html
  # Use Records for message struct instead of pure Maps

  def get_error_types() do
    %{
      generic_error: 201,
      server_error: 202,
      protocol_error: 203,
      method_unknown: 204
    }
  end

  def get_sender_id(msg) do
    # Extract the message sender's node_id.
    # -> <<binary node_id>> | nil
    case msg["y"] do
      "q" -> msg["a"]["id"]
      "r" -> msg["r"]["id"]
      "e" -> nil
    end
  end

  def error_code(error_type) do
    Enum.fetch!(get_error_types(), error_type)
  end

  def to_bin(msg) when is_map(msg) do
    Bencoding.encode(msg)
  end

  def from_bin(data) when is_binary(data) do
    {:ok, {msg, <<"">>}} = from_buffer(data)
    msg
  end

  def from_buffer(buffer) when is_binary(buffer) do
    try do
      {msg, rest_buff} = Bencoding.decode_prefix_dict(buffer, %{decode_string_as: :ascii})
      Logger.debug("[KRPC_Msg.from_buffer] Got msg #{inspect(msg)}")
      {:ok, {msg, rest_buff}}
    rescue
      _e ->
        # TODO: Do not catch all excpetions
        {:error, :invalid_format}
    end
  end

  def ping(sender_id) do
    # ping Query = {"t":"aa", "y":"q", "q":"ping", "a":{"id":"abcdefghij0123456789"}}
    # bencoded = d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe
    # Response = {"t":"aa", "y":"r", "r": {"id":"mnopqrstuvwxyz123456"}}
    # bencoded = d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re
    query_args = %{"id" => sender_id}
    %{"t" => gen_transaction_id(), "y" => "q", "q" => "ping", "a" => query_args}
  end

  def find_node(sender_id, target_node_id) do
    query_args = %{"id" => sender_id, "target" => target_node_id}
    %{"t" => gen_transaction_id(), "y" => "q", "q" => "find_node", "a" => query_args}
  end

  def get_peers(sender_id, torrent_info_hash) do
    query_args = %{"id" => sender_id, "info_hash" => torrent_info_hash}
    %{"t" => gen_transaction_id(), "y" => "q", "q" => "get_peers", "a" => query_args}
  end

  def announce_peer(sender_id, torrent_info_hash, token, port, implied_port \\ 0) do
    query_args = %{
      "id" => sender_id,
      "implied_port" => implied_port,
      "info_hash" => torrent_info_hash,
      "port" => port,
      "token" => token
    }

    %{"t" => gen_transaction_id(), "y" => "q", "q" => "announce_peer", "a" => query_args}
  end

  def error(transaction_id, error_code, error_msg) do
    if error_code not in Enum.values(get_error_types()) do
      raise "Unknown error_code: #{error_code}"
    end

    %{"t" => transaction_id, "y" => "e", "e" => [error_code, error_msg]}
  end

  def response(sender_id, transaction_id, response_data) do
    %{
      "t" => transaction_id,
      "y" => "r",
      # Include the queried node's ID
      "r" => Map.put(response_data, "id", sender_id)
    }
  end

  def gen_transaction_id do
    # The transaction ID should be encoded as a short string of binary numbers,
    # typically 2 characters are enough as they cover 2^16 outstanding queries.
    :crypto.strong_rand_bytes(2) |> Base.url_encode64() |> binary_part(0, 2)
  end
end

defmodule TokenServer do
  # Provodes tokens for the KRPC's secret_length/announce_peer methods
  use GenServer

  @token_expire_time 5 * 60 * 1000
  @secret_length 8

  defstruct [
    :secret,
    :old_secret
  ]

  def start_link(init_args, debug \\ false) do
    opts =
      if debug == true do
        [debug: [:statistics, :trace]]
      else
        []
      end

    GenServer.start_link(__MODULE__, init_args, opts)
  end

  def init([]) do
    state = %TokenServer{secret: random_secret(), old_secret: random_secret()}
    {:ok, state}
  end

  def is_valid_token?(pid, ip, token) do
    GenServer.call(pid, {:is_valid_token, ip, token})
  end

  def request_token(pid, ip) do
    GenServer.call(pid, {:request_token, ip})
  end

  def random_secret() do
    :crypto.strong_rand_bytes(@secret_length)
  end

  def calculate_token(ip, secret) do
    {a, b, c, d} = ip
    data = <<a, b, c, d>> <> secret
    :crypto.hash(:sha, data) |> Base.encode16() |> String.downcase()
  end

  def handle_call({:is_valid_token, ip, token}, _from, state) do
    is_valid =
      token == calculate_token(ip, state.secret) or token == calculate_token(ip, state.old_secret)

    {:reply, is_valid, state}
  end

  def handle_call({:request_token, ip}, _from, state) do
    token = calculate_token(ip, state.secret)
    {:reply, token, state}
  end

  def handle_info(:loop_renew_secret, state) do
    new_state = %{state | old_secret: state.secret, secret: random_secret()}
    Process.send_after(self(), :loop_renew_secret, @token_expire_time)
    {:noreply, new_state}
  end
end

defmodule DHTServer do
  use GenServer
  require Logger
  @ping_response_threshold_sec 30
  @krpc_query_timeout 6000
  @api_general_timeout 8000
  @api_search_nodes_timeout 30000

  defstruct [
    :node_id,
    :table,
    :peer_info,
    :token_server,
    :socket,
    :waiting_response
  ]

  def start(init_args, debug \\ false) do
    opts =
      if debug == true do
        [debug: [:statistics, :trace]]
      else
        []
      end

    GenServer.start(__MODULE__, init_args, opts)
  end

  def start_link(init_args, debug \\ false) do
    opts =
      if debug == true do
        [debug: [:statistics, :trace]]
      else
        []
      end

    GenServer.start_link(__MODULE__, init_args, opts)
  end

  def init([node_id, port]) do
    {:ok, socket} = :gen_udp.open(port, [:binary, :inet, {:active, true}])
    {:ok, token_server_pid} = TokenServer.start_link([])

    state = %DHTServer{
      node_id: node_id,
      table: DHTTable.new_table(node_id),
      peer_info: %{},
      token_server: token_server_pid,
      waiting_response: %{},
      socket: socket
    }
    GenServer.cast(self(), :bootstrap)
    {:ok, state}
  end

  def ping(pid, node_id) do
    # -> :ok | {:error, :response_timeout}
    GenServer.call(pid, {:ping, node_id}, @api_general_timeout)
  end

  def find_node(pid, node_id, receiver_node_id) do
    # -> <node_list> or {:error, :response_timeout}
    GenServer.call(pid, {:find_node, node_id, receiver_node_id}, @api_general_timeout)
  end

  def find_node(pid, node_id, receiver_ip, receiver_port) do
    GenServer.call(pid, {:find_node, node_id, receiver_ip, receiver_port}, @api_general_timeout)
  end

  def get_peers(pid, info_hash, receiver_ip, receiver_port) do
    # ->  {:peers, <peer_list>}
    #  or {:nodes, <node_list>}
    #  or {:error, :response_timeout}
    GenServer.call(pid, {:get_peers, info_hash, receiver_ip, receiver_port}, @api_general_timeout)
  end

  def node_id(pid) do
    GenServer.call(pid, :node_id)
  end

  def search_peers(pid, info_hash) do
    # -> {:ok, <list of peers>} or {:error, <reason>}
    if bit_size(info_hash) != 160 do
      {:error, "Invalid size of info_hash"}
    else
      neighbor_nodes = GenServer.call(pid, {:stored_nearest_nodes, info_hash})
      Logger.debug("[search_peers] picked neighbor_nodes #{inspect(neighbor_nodes)}")
      acc = {neighbor_nodes, [], []}
      peers = search_peers(pid, info_hash, acc)
      {:ok, peers}
    end
  end

  def search_peers(pid, info_hash, acc) do
    Logger.debug("[DHTServer] search peers of info_hash #{inspect(info_hash)}")
    {neighbor_nodes, searched_ids, acc_peers} = acc

    nodes_to_search =
      Enum.filter(
        Enum.take(neighbor_nodes, 3),
        fn x -> x.id not in searched_ids end
      )

    if nodes_to_search == [] do
      acc_peers
    else
      {new_nodes, new_peers} =
        Task.async_stream(
          nodes_to_search,
          fn x ->
            GenServer.call(pid, {:get_peers, info_hash, x.ip, x.port}, :infinity)
          end,
          [{:timeout, @api_general_timeout}]
        )
        |> Enum.to_list()
        |> List.foldl(
          {[], []},
          fn x, {nodes, peers} = acc ->
            case x do
              {:error, reason} ->
                Logger.warn("[search_peers] task error: #{inspect(reason)}")
                acc

              {:exit, reason} ->
                Logger.warn("[search_peers] task exit: #{inspect(reason)}")
                acc

              {:ok, {:error, reason}} ->
                Logger.warn("[search_peers] krpc error: #{inspect(reason)}")
                acc

              {:ok, {:nodes, ns}} ->
                {nodes ++ ns, peers}

              {:ok, {:peers, ps}} ->
                {nodes, peers ++ ps}
            end
          end
        )
      Logger.debug("new_nodes=#{inspect(new_nodes)}, new_peers=#{inspect(new_peers)}")
      neighbor_nodes2 =
        (new_nodes ++ neighbor_nodes)
        |> Enum.uniq_by(fn x -> x.id end)
        |> Enum.sort_by(fn x -> DHTUtils.node_distance_binary(info_hash, x.id) end)

      searched_ids2 = searched_ids ++ Enum.map(nodes_to_search, fn x -> x.id end)
      acc2 = {neighbor_nodes2, searched_ids2, acc_peers ++ new_peers}
      search_peers(pid, info_hash, acc2)
    end
  end

  def search_nodes(pid, target_node_id) do
    # returns a list of close nodes
    neighbor_nodes = GenServer.call(pid, {:stored_nearest_nodes, target_node_id})
    Logger.debug("[search_nodes] picked neighbor_nodes #{inspect(neighbor_nodes)}")
    acc = {neighbor_nodes, []}
    search_nodes(pid, target_node_id, acc)
  end

  def search_nodes(pid, target_node_id, acc) do
    {neighbor_nodes, searched_ids} = acc

    nodes_to_search =
      Enum.filter(
        Enum.take(neighbor_nodes, 3),
        fn x -> x.id not in searched_ids end
      )

    if nodes_to_search == [] do
      Enum.take(neighbor_nodes, 3)
    else
      new_nodes =
        Task.async_stream(
          nodes_to_search,
          fn x ->
            case find_node(pid, target_node_id, x.ip, x.port) do
              {:error, _reason} -> []
              nodes -> nodes
            end
          end,
          [{:timeout, @api_search_nodes_timeout}]
        )
        |> Enum.to_list()
        |> List.foldl(
          [],
          fn x, acc ->
            case x do
              {:ok, nodes} -> nodes ++ acc
              _ -> acc
            end
          end
        )

      # Enum.each(new_nodes, fn x -> add_node(pid) end)
      Logger.debug("[search_nodes] new_nodes: #{inspect(new_nodes)}")

      neighbor_nodes2 =
        (new_nodes ++ neighbor_nodes)
        |> Enum.uniq_by(fn x -> x.id end)
        |> Enum.sort_by(fn x -> DHTUtils.node_distance_binary(target_node_id, x.id) end)

      searched_ids2 = searched_ids ++ Enum.map(nodes_to_search, fn x -> x.id end)
      acc2 = {neighbor_nodes2, searched_ids2}
      search_nodes(pid, target_node_id, acc2)
    end
  end

  def schedule_node_check(pid, node_id) do
    Process.send_after(pid, {:check_node_aliveness, node_id}, @ping_response_threshold_sec)
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:node_id, _from, state) do
    {:reply, state.table.reference_node_id, state}
  end

  def handle_call({:ping, node_id}, from, state) do
    case DHTTable.get_node(state.table, node_id) do
      nil ->
        {:reply, {:error, :node_notfound}, state}

      node ->
        msg = KRPC_Msg.ping(state.node_id)
        handle_call({:send_krpc_msg, node.ip, node.port, msg}, from, state)
    end
  end

  def handle_call({:add_node, node_id, ip, port}, _from, state) do
    new_state = do_add_node(node_id, ip, port, state)
    {:reply, :ok, new_state}
  end

  def handle_call({:find_node, node_id, receiver_node_id}, from, state) do
    # send a find_node query to <receiver_node_id>
    case DHTTable.get_node(state.table, receiver_node_id) do
      nil ->
        {:reply, {:error, "unknown receiver_node_id", receiver_node_id}}

      node ->
        handle_call({:find_node, node_id, node.ip, node.port}, from, state)
    end
  end

  def handle_call({:find_node, node_id, receiver_ip, receiver_port}, from, state) do
    # send a find_node query to <receiver_node_id>
    msg = KRPC_Msg.find_node(state.node_id, node_id)
    handle_call({:send_krpc_msg, receiver_ip, receiver_port, msg}, from, state)
  end

  def handle_call({:get_peers, info_hash, receiver_ip, receiver_port}, from, state) do
    msg = KRPC_Msg.get_peers(state.node_id, info_hash)
    handle_call({:send_krpc_msg, receiver_ip, receiver_port, msg}, from, state)
  end

  def handle_call({:stored_nearest_nodes, node_id}, _from, state) do
    # returns k-nearest nodes. sorted by distance (the closest first)
    sorted_nodes = DHTTable.get_k_nearest_nodes(state.table, node_id)

    {:reply, sorted_nodes, state}
  end

  def handle_call({:send_krpc_msg, ip, port, msg}, from, state) do
    send_krpc_msg!(state.socket, ip, port, msg)

    if msg["y"] == "q" do
      # Save the query-msg for further checks
      transaction_id = msg["t"]
      waiting = Map.put(state.waiting_response, transaction_id, {msg, from})
      new_state = %{state | waiting_response: waiting}
      Process.send_after(self(), {:krpc_query_timeout, transaction_id}, @krpc_query_timeout)
      # Suspend the :reply until we receives the query-response
      {:noreply, new_state}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call(unknown_msg, _from, state) do
    Logger.error("[DHTServer] Unknown handle_call msg: #{inspect(unknown_msg)}")
    {:reply, :unknown_msg, state}
  end

  def handle_cast(:bootstrap, state) do
    # TODO: make it configureable
    handle_cast({:bootstrap, {67, 215, 246, 10}, 6881}, state)
  end

  def handle_cast({:bootstrap, ip, port}, state) do
    # Bootstrap by performing a node-lookup. Searching for itself.
    this = self()

    Task.start_link(fn ->
      case find_node(this, state.node_id, ip, port) do
        {:error, reason} ->
          Logger.error("[bootstrap] failed to find_node: #{inspect(reason)}")

        nodes ->
          Logger.info("[bootstrap] got nodes from bootstrap node: #{inspect(nodes)}")

          Enum.each(nodes, fn x ->
            GenServer.call(this, {:add_node, x.id, x.ip, x.port}, :infinity)
          end)

          res = search_nodes(this, state.node_id)
          Logger.info("[bootstrap] result: #{inspect(res)}")
      end
    end)

    {:noreply, state}
  end

  def handle_info({:udp, _socket, ip, port, raw}, state) do
    Logger.debug("[udp] #{inspect(ip)}:#{port} >>> Me: #{raw}")

    decode_result =
      try do
        msg = KRPC_Msg.from_bin(raw)
        {:ok, msg}
      rescue
        err ->
          Logger.error(Exception.format(:error, err, __STACKTRACE__))
          {:error, err}
      end

    case decode_result do
      {:ok, msg} ->
        send(self(), {:krpc, ip, port, msg})

      {:error, err} ->
        Logger.error("Failed to decode data: #{inspect(err)}. Drop this packet.")
        File.write!("/tmp/dht_malformed.bin", raw)
        Logger.error("Malformed packet has been writting to dht_malformed.bin")
    end

    {:noreply, state}
  end

  def handle_info({:krpc, ip, port, msg}, state) when is_map(msg) do
    node_id = KRPC_Msg.get_sender_id(msg)

    state2 =
      if node_id != nil do
        do_add_node(node_id, ip, port, state)
      else
        state
      end

    state3 = do_on_krpc_msg(ip, port, msg, state2)
    {:noreply, state3}
  end

  def handle_info({:check_node_aliveness, node_id}, state) do
    # If the nodes's last_active time exceed threshold, delete it.
    # Also add one candidate-node
    bucket = DHTTable.get_corresponding_bucket(state.table, node_id)

    case DHTBucket.get_node_by_id(bucket, node_id) do
      nil ->
        {:noreply, state}

      node ->
        if System.system_time(:second) - node.last_active > @ping_response_threshold_sec do
          updated_bucket = DHTBucket.delete_node(bucket, node.id)
          bucket_num = DHTTable.get_corresopnding_bucket_num(state.table, node.id)
          t = List.replace_at(state.table.buckets, bucket_num, updated_bucket)
          new_state = %{state | table: t}
          {:noreply, new_state}
        else
          {:noreply, state}
        end
    end
  end

  def handle_info({:krpc_query_timeout, transaction_id}, state) do
    case Map.pop(state.waiting_response, transaction_id) do
      {nil, _m} ->
        {:noreply, state}

      {{req, req_pid}, m} ->
        Logger.warn(
          "krpc_query_timeout: delete #{inspect(transaction_id)} from waiting queue: #{
            inspect(req)
          }"
        )

        GenServer.reply(req_pid, {:error, :response_timeout})
        new_state = %{state | waiting_response: m}
        {:noreply, new_state}
    end
  end

  def handle_info(msg, state) do
    Logger.error("Unknown handle_info msg: #{inspect(msg)}")
    {:noreply, state}
  end

  def do_add_node(node_id, ip, port, state) do
    # -> state | new_state
    Logger.debug("[do_add_node] adding node #{inspect(node_id)} #{inspect(ip)}:#{port}")
    distance = DHTUtils.node_distance_binary(state.table.reference_node_id, node_id)
    num = DHTTable.get_bucket_num_by_distance(distance)
    Logger.debug("[do_add_node] dist #{inspect(distance)}, bucket #{num}")
    target_bucket = DHTTable.get_bucket_by_distance(state.table, distance)
    node = %DHTNode{id: node_id, ip: ip, port: port, last_active: System.system_time(:second)}

    case DHTBucket.add_node(target_bucket, node) do
      {:ok, updated_bucket} ->
        updated_table = %{
          state.table
          | buckets: List.replace_at(state.table.buckets, num, updated_bucket)
        }

        new_state = %{state | table: updated_table}
        new_state

      {:error, :bucket_full} ->
        Logger.warn("[do_add_node] bucket full: #{inspect(target_bucket)}")

        case DHTBucket.get_most_questionable_node(target_bucket) do
          nil ->
            # All nodes are good. Preserve long-lived nodes.
            state

          x ->
            if length(target_bucket.candidates) == target_bucket.capacity do
              state
            else
              bucket = DHTBucket.add_candidate(target_bucket, node)

              updated_table = %{
                state.table
                | buckets: List.replace_at(state.table.buckets, num, bucket)
              }

              this = self()
              spawn_link(fn -> ping(this, x.id) end)
              schedule_node_check(self(), x.id)
              new_state = %{state | table: updated_table}
              new_state
            end
        end
    end
  end

  def send_krpc_msg!(socket, ip, port, msg) do
    Logger.debug("[send_krpc_msg!] #{inspect(msg)}")
    msg_bin = Bencoding.encode(msg)
    Logger.debug("[udp] Me >>> #{inspect(ip)}:#{port}: #{inspect(msg_bin)}")
    :ok = :gen_udp.send(socket, ip, port, msg_bin)
  end

  def do_on_krpc_msg(ip, port, %{"y" => "q", "q" => "ping"} = msg_ping, state) do
    msg = KRPC_Msg.response(state.node_id, msg_ping["t"], %{})
    send_krpc_msg!(state.socket, ip, port, msg)
    state
  end

  def do_on_krpc_msg(ip, port, %{"y" => "q", "q" => "find_node"} = msg_find_node, state) do
    nodes = DHTTable.get_k_nearest_nodes(state.table, msg_find_node["a"]["target"], 8)
    node_info = Enum.map(nodes, fn x -> DHTNode.compact_info(x) end) |> Enum.join()
    resp_data = %{"nodes" => node_info}
    msg = KRPC_Msg.response(state.node_id, msg_find_node["t"], resp_data)
    send_krpc_msg!(state.socket, ip, port, msg)
    state
  end

  def do_on_krpc_msg(ip, port, %{"y" => "q", "q" => "get_peers"} = msg_get_peers, state) do
    info_hash = msg_get_peers["a"]["info_hash"]
    token = TokenServer.request_token(state.token_server, ip)

    resp_data =
      case state.peer_info[info_hash] do
        nil ->
          nodes = DHTTable.get_k_nearest_nodes(state.table, info_hash)
          node_info = Enum.map(nodes, fn x -> DHTNode.compact_info(x) end) |> Enum.join()
          %{"nodes" => node_info, "token" => token}

        values ->
          %{"values" => values, "token" => token}
      end

    msg = KRPC_Msg.response(state.node_id, msg_get_peers["t"], resp_data)
    send_krpc_msg!(state.socket, ip, port, msg)
    state
  end

  def do_on_krpc_msg(ip, port, %{"y" => "q", "q" => "announce_peer"} = msg_announce_peer, state) do
    token = msg_announce_peer["a"]["token"]
    transaction_id = msg_announce_peer["t"]

    if not TokenServer.is_valid_token?(state.token_server, ip, token) do
      msg =
        KRPC_Msg.error(
          transaction_id,
          KRPC_msg.error_code(:protocol_error),
          "Invalid Token"
        )

      send_krpc_msg!(state.socket, ip, port, msg)
      state
    else
      peer_bt_port =
        if Map.get(msg_announce_peer["a"], "implied_port", 0) != 0 do
          # Use source port
          port
        else
          msg_announce_peer["a"]["port"]
        end

      {a, b, c, d} = ip
      info = <<a, b, c, d>> <> <<peer_bt_port::integer-size(16)>>
      info_list = Map.get(state.peer_info, msg_announce_peer["a"]["info_hash"], [])
      new_state = %{state | peer_info: Map.put(state.peer_info, [info | info_list])}
      new_state
    end
  end

  def do_on_krpc_msg(_ip, _port, %{"y" => "r"} = msg_response, state) do
    # Deal with various type of responses
    # ip, port: received this response-message from <ip>:<port>
    transaction_id = msg_response["t"]

    if Map.has_key?(state.waiting_response, transaction_id) do
      {{corresponding_request, from_pid}, rest} = Map.pop(state.waiting_response, transaction_id)

      Logger.debug(
        "Found matching request #{inspect(corresponding_request)} >>> #{inspect(msg_response)}"
      )

      case corresponding_request do
        %{"q" => "ping"} ->
          GenServer.reply(from_pid, :ok)

        %{"q" => "find_node"} ->
          nodes_string = Map.get(msg_response["r"], "nodes", "")
          nodes = DHTUtils.parse_nodes_val(nodes_string)
          GenServer.reply(from_pid, nodes)

        %{"q" => "get_peers"} ->
          case msg_response do
            %{"r" => %{"values" => peers_val}} ->
              peers = DHTUtils.parse_peers_val(peers_val)
              GenServer.reply(from_pid, {:peers, peers})

            %{"r" => %{"nodes" => nodes_val}} ->
              nodes = DHTUtils.parse_nodes_val(nodes_val)
              GenServer.reply(from_pid, {:nodes, nodes})
          end

        %{"q" => "announce_peer"} ->
          GenServer.reply(from_pid, :ok)
      end

      new_state = %{state | waiting_response: rest}
      new_state
    else
      Logger.warn("(skip msg) Cannot find corresponsding query of #{inspect(msg_response)}.")
      state
    end
  end

  def do_on_krpc_msg(ip, port, %{"y" => "e"} = msg_error, state) do
    [err_code, err_msg] = msg_error["e"]
    transaction_id = msg_error["t"]

    if Map.has_key?(state.waiting_response, transaction_id) do
      {{req, from_pid}, rest_waiting} = Map.pop(state.waiting_response, transaction_id)
      new_state = %{state | waiting_response: rest_waiting}
      Logger.error(
        "[KRPC-error] #{inspect(ip)}:#{port} request: #{inspect(req)}. error: #{inspect(msg_error)}"
      )
      GenServer.reply(from_pid, {:error, {err_code, err_msg}})
      new_state
    else
      state
    end

  end

  def do_on_krpc_msg(ip, port, %{"y" => "q", "q" => unknown} = msg_unknown_query, state) do
    transaction_id = msg_unknown_query["t"]

    msg =
      KRPC_Msg.error(
        transaction_id,
        KRPC_msg.error_code(:method_unknown),
        "Unknown query method: #{unknown}"
      )

    send_krpc_msg!(state.socket, ip, port, msg)
    state
  end

  def do_on_krpc_msg(ip, port, unknown_msg, state) do
    Logger.warn("(Skip) Got unknown KRPC msg: #{inspect(unknown_msg)}")

    if Map.has_key?(unknown_msg, "t") do
      msg =
        KRPC_Msg.error(
          unknown_msg["t"],
          KRPC_msg.error_code(:protocol_error),
          "Unknown message format"
        )

      send_krpc_msg!(state.socket, ip, port, msg)
    else
      :do_nothing
    end

    state
  end
end

defmodule PeerDiscovery do
  use GenServer
  require Logger

  @peer_search_interval 30000

  # GenServer state
  defstruct [
    sup_pid: nil,
    found_peers: [],
    torrent_info_hash: nil,
  ]

  def start(init_args) do
    GenServer.start(__MODULE__, init_args)
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  def init([my_p2p_id, torrent_info_hash, dht_port]=_init_args) do

    children = [
      %{id: DHTServer, start: {DHTServer, :start_link, [[my_p2p_id, dht_port]]}},
    ]
    {:ok, sup_pid} = Supervisor.start_link(children, strategy: :one_for_one)

    init_state = %PeerDiscovery{
                    sup_pid: sup_pid,
                    torrent_info_hash: torrent_info_hash,
                  }
    Logger.info("[PeerDiscover] initialized: #{inspect(init_state)}")
    {:ok, init_state, {:continue, :enable_peer_search}}
  end

  def handle_continue(:enable_peer_search, state) do
    send(self(), {:dht_search_peers, []})
    {:noreply, state}
  end

  def handle_call(:found_peers, _from, state) do
    {:reply, state.found_peers, state}
  end

  def handle_call({:start_tracker, my_p2p_id, fileserver_pid, tracker_port}, _from, state) do
    if GenServer.call(fileserver_pid, :decoded_torrent) == nil do
      {:reply, {:error, :no_metadata}, state}
    else
      child_spec = %{id: TrackerClient,
                     start: {TrackerClient, :start_link, [[my_p2p_id, tracker_port, fileserver_pid]]},
                     restart: :transient,
                    }
      Supervisor.start_child(state.sup_pid, child_spec)
      {:reply, :ok, state}
    end
  end

  def handle_info({:dht_search_peers, peers}, state) do
    # <peers>: from DHT get_peers
    Logger.debug("got peers from dht: #{inspect(peers)}")
    found_peers = (peers ++ state.found_peers) |> Enum.uniq
    new_state = %{state | found_peers: found_peers}

    # Prepare the next round
    peer_discovery_pid = self()
    dht_pid = get_service(state.sup_pid, :dht)
    spawn_link(fn ->
      Process.sleep(@peer_search_interval)
      {:ok, peers} = DHTServer.search_peers(dht_pid, state.torrent_info_hash)
      send(peer_discovery_pid, {:dht_search_peers, peers})
    end)

    {:noreply, new_state}
  end

  def handle_info({:tracker_search_status, peers}, state) do
    # <peers>: from Tracker Client
    Logger.debug("got peers from tracker: #{inspect(peers)}")

    found_peers = [peers | state.found_peers] |> Enum.uniq
    new_state = %{state | found_peers: found_peers}

    # Prepare the next round
    tracker_client = get_service(state.sup_pid, :tracker)
    peer_discovery_pid = self()
    spawn_link(fn ->
      Process.sleep(@peer_search_interval)
      peers = GenServer.call(tracker_client, :peer_network_status)["peers"]
      send(peer_discovery_pid, {:tracker_search_status, peers})
    end)

    {:noreply, new_state}
  end

  def get_service(supervisor_pid, service) do
    # Fetch the service's pid from its supervisor
    # service: :dht or :tracker
    # -> <service_pid>

    children = Supervisor.which_children(supervisor_pid)
    case service do
      :dht ->
        case List.keyfind(children, DHTServer, 0) do
          {DHTServer, dht_pid, _, _} -> dht_pid
          nil -> nil
        end
      :tracker ->
        case List.keyfind(children, TrackerClient, 0) do
          {TrackerClient, tracker_client_pid, _, _} -> tracker_client_pid
          nil -> nil
        end
    end
  end

end

IO.puts("======== Running Tests ==========")
Bencoding.test_all()
FileServer.test_all()
Connector.test_all()
Job.test_all()
Job.check_requirements()
IO.puts("======== all test passed ========")
