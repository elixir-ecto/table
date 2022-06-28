defmodule Table do
  @moduledoc """
  Unified access to tabular data.

  Various data structures have a tabular representation, however to
  access this representation, manual conversion is required. On top
  of that, tabular access itself has two variants, a row-based one
  and a column-based one, each useful under different circumstances.

  The `Table` package provides a thin layer that unifies access to
  tabular data in different formats.

  ## Protocol

  The unified access is enabled for structs implementing the
  `Table.Reader` protocol. Note that a struct may be representable
  as tabular data only in some cases, so the protocol implementation
  may be lax. Consequently, functions in this module will raise when
  given non-tabular data.

  By default the protocol is implemented for lists and maps of certain
  shape.

      # List of matching key-value lists
      data = [
        [{"id", 1}, {"name", "Sherlock"}],
        [{"id", 2}, {"name", "John"}]
      ]

      # List of matching maps
      data = [
        %{"id" => 1, "name" => "Sherlock"},
        %{"id" => 2, "name" => "John"}
      ]

      # List of column tuples
      data = [
        {"id", 1..2},
        {"name", ["Sherlock", "John"]}
      ]

      # Map with column values
      data = %{
        "id" => [1, 2],
        "name" => ["Sherlock", "John"]
      }

  """

  alias Table.Reader

  @type column :: term()

  @type tabular :: Reader.t() | Reader.row_reader() | Reader.column_reader()

  @doc """
  Accesses tabular data as a sequence of rows.

  Returns an enumerable that emits each row as a map.

  ## Options

    * `:only` - specifies a subset of columns to include in the result

  ## Examples

      iex> data = %{id: [1, 2, 3], name: ["Sherlock", "John", "Mycroft"]}
      iex> data |> Table.to_rows() |> Enum.to_list()
      [%{id: 1, name: "Sherlock"}, %{id: 2, name: "John"}, %{id: 3, name: "Mycroft"}]

      iex> data = [[id: 1, name: "Sherlock"], [id: 2, name: "John"], [id: 3, name: "Mycroft"]]
      iex> data |> Table.to_rows() |> Enum.to_list()
      [%{id: 1, name: "Sherlock"}, %{id: 2, name: "John"}, %{id: 3, name: "Mycroft"}]

  """
  @spec to_rows(tabular(), keyword()) :: Enumerable.t()
  def to_rows(tabular, opts \\ []) do
    only = opts[:only] && MapSet.new(opts[:only])

    tabular
    |> init_reader!()
    |> read_rows(only)
  end

  # TODO: remove in v0.2
  @deprecated "Use Table.Reader.init/1 to get reader with metadata, then pass the reader to Table.to_rows/2"
  def to_rows_with_info(tabular, opts \\ []) do
    reader = {_, meta, _} = Table.Reader.init(tabular)
    {to_rows(reader, opts), meta}
  end

  defp init_reader!({:rows, %{}, _} = reader), do: reader
  defp init_reader!({:columns, %{}, _} = reader), do: reader

  defp init_reader!(tabular) do
    with :none <- Reader.init(tabular) do
      raise ArgumentError, "expected valid tabular data, but got: #{inspect(tabular)}"
    end
  end

  defp read_rows({:rows, meta, enum}, only) do
    Table.Mapper.map(enum, fn values ->
      build_row(meta.columns, values, only)
    end)
  end

  defp read_rows({:columns, meta, enum}, only) do
    stream_zip_with(enum, fn values ->
      build_row(meta.columns, values, only)
    end)
  end

  defp build_row(columns, values, only) do
    for {column, value} <- Enum.zip(columns, values),
        include_column?(only, column),
        into: %{},
        do: {column, value}
  end

  @doc """
  Accesses tabular data as individual columns.

  Returns a map with enumerables as values.

  ## Options

    * `:only` - specifies a subset of columns to include in the result

  ## Examples

      iex> data = [%{id: 1, name: "Sherlock"}, %{id: 2, name: "John"}, %{id: 3, name: "Mycroft"}]
      iex> columns = Table.to_columns(data)
      iex> Enum.to_list(columns.id)
      [1, 2, 3]
      iex> Enum.to_list(columns.name)
      ["Sherlock", "John", "Mycroft"]

  """
  @spec to_columns(tabular(), keyword()) :: %{column() => Enumerable.t()}
  def to_columns(tabular, opts \\ []) do
    only = opts[:only] && MapSet.new(opts[:only])

    tabular
    |> init_reader!()
    |> read_columns(only)
  end

  # TODO: remove in v0.2
  @deprecated "Use Table.Reader.init/1 to get reader with metadata, then pass the reader to Table.to_columns/2"
  def to_columns_with_info(tabular, opts \\ []) do
    reader = {_, meta, _} = Table.Reader.init(tabular)
    {to_columns(reader, opts), meta}
  end

  defp read_columns({:columns, meta, enum}, only) do
    for {column, values} <- Enum.zip(meta.columns, enum),
        include_column?(only, column),
        into: %{},
        do: {column, values}
  end

  defp read_columns({:rows, meta, enum}, only) do
    columns =
      for {column, idx} <- Enum.with_index(meta.columns),
          include_column?(only, column),
          do: {column, idx, []}

    # Note: we intentionally materialize the columns into memory,
    # because having a separate stream for each column would be
    # notably less efficient on the consumer side
    columns = Enum.reduce(enum, columns, &row_into_columns/2)

    for {column, _, acc} <- columns,
        into: %{},
        do: {column, Enum.reverse(acc)}
  end

  defp row_into_columns(row, columns), do: row_into_columns(row, 0, columns)

  defp row_into_columns([value | values], idx, [{column, idx, acc} | columns]) do
    [{column, idx, [value | acc]} | row_into_columns(values, idx + 1, columns)]
  end

  defp row_into_columns([_value | values], idx, columns) do
    row_into_columns(values, idx + 1, columns)
  end

  defp row_into_columns([], _idx, []), do: []

  defp include_column?(nil, _column), do: true
  defp include_column?(only, column), do: MapSet.member?(only, column)

  # --- Backports ---

  # TODO: remove once we require Elixir v1.12
  # Source https://github.com/elixir-lang/elixir/blob/b63f8f541e9d8951dbbcb39a8551bd74a3fe9a59/lib/elixir/lib/stream.ex#L1210-L1342
  defp stream_zip_with(enumerables, zip_fun) when is_function(zip_fun, 1) do
    if is_list(enumerables) and :lists.all(&is_list/1, enumerables) do
      &zip_list(enumerables, &1, &2, zip_fun)
    else
      &zip_enum(enumerables, &1, &2, zip_fun)
    end
  end

  defp zip_list(_enumerables, {:halt, acc}, _fun, _zip_fun) do
    {:halted, acc}
  end

  defp zip_list(enumerables, {:suspend, acc}, fun, zip_fun) do
    {:suspended, acc, &zip_list(enumerables, &1, fun, zip_fun)}
  end

  defp zip_list(enumerables, {:cont, acc}, fun, zip_fun) do
    case zip_list_heads_tails(enumerables, [], []) do
      {heads, tails} -> zip_list(tails, fun.(zip_fun.(heads), acc), fun, zip_fun)
      :error -> {:done, acc}
    end
  end

  defp zip_list_heads_tails([[head | tail] | rest], heads, tails) do
    zip_list_heads_tails(rest, [head | heads], [tail | tails])
  end

  defp zip_list_heads_tails([[] | _rest], _heads, _tails) do
    :error
  end

  defp zip_list_heads_tails([], heads, tails) do
    {:lists.reverse(heads), :lists.reverse(tails)}
  end

  defp zip_enum(enumerables, acc, fun, zip_fun) do
    step = fn x, acc ->
      {:suspend, :lists.reverse([x | acc])}
    end

    enum_funs =
      Enum.map(enumerables, fn enum ->
        {&Enumerable.reduce(enum, &1, step), [], :cont}
      end)

    do_zip_enum(enum_funs, acc, fun, zip_fun)
  end

  # This implementation of do_zip_enum/4 works for any number of streams to zip
  defp do_zip_enum(zips, {:halt, acc}, _fun, _zip_fun) do
    do_zip_close(zips)
    {:halted, acc}
  end

  defp do_zip_enum(zips, {:suspend, acc}, fun, zip_fun) do
    {:suspended, acc, &do_zip_enum(zips, &1, fun, zip_fun)}
  end

  defp do_zip_enum([], {:cont, acc}, _callback, _zip_fun) do
    {:done, acc}
  end

  defp do_zip_enum(zips, {:cont, acc}, callback, zip_fun) do
    try do
      do_zip_next(zips, acc, callback, [], [], zip_fun)
    catch
      kind, reason ->
        do_zip_close(zips)
        :erlang.raise(kind, reason, __STACKTRACE__)
    else
      {:next, buffer, acc} ->
        do_zip_enum(buffer, acc, callback, zip_fun)

      {:done, _acc} = other ->
        other
    end
  end

  # do_zip_next/6 computes the next tuple formed by
  # the next element of each zipped stream.
  defp do_zip_next(
         [{_, [], :halt} | zips],
         acc,
         _callback,
         _yielded_elems,
         buffer,
         _zip_fun
       ) do
    do_zip_close(:lists.reverse(buffer, zips))
    {:done, acc}
  end

  defp do_zip_next([{fun, [], :cont} | zips], acc, callback, yielded_elems, buffer, zip_fun) do
    case fun.({:cont, []}) do
      {:suspended, [elem | next_acc], fun} ->
        next_buffer = [{fun, next_acc, :cont} | buffer]
        do_zip_next(zips, acc, callback, [elem | yielded_elems], next_buffer, zip_fun)

      {_, [elem | next_acc]} ->
        next_buffer = [{fun, next_acc, :halt} | buffer]
        do_zip_next(zips, acc, callback, [elem | yielded_elems], next_buffer, zip_fun)

      {_, []} ->
        # The current zipped stream terminated, so we close all the streams
        # and return {:halted, acc} (which is returned as is by do_zip/3).
        do_zip_close(:lists.reverse(buffer, zips))
        {:done, acc}
    end
  end

  defp do_zip_next(
         [{fun, zip_acc, zip_op} | zips],
         acc,
         callback,
         yielded_elems,
         buffer,
         zip_fun
       ) do
    [elem | rest] = zip_acc
    next_buffer = [{fun, rest, zip_op} | buffer]
    do_zip_next(zips, acc, callback, [elem | yielded_elems], next_buffer, zip_fun)
  end

  defp do_zip_next([] = _zips, acc, callback, yielded_elems, buffer, zip_fun) do
    # "yielded_elems" is a reversed list of results for the current iteration of
    # zipping. That is to say, the nth element from each of the enums being zipped.
    # It needs to be reversed and passed to the zipping function so it can do it's thing.
    {:next, :lists.reverse(buffer), callback.(zip_fun.(:lists.reverse(yielded_elems)), acc)}
  end

  defp do_zip_close(zips) do
    :lists.foreach(fn {fun, _, _} -> fun.({:halt, []}) end, zips)
  end
end
