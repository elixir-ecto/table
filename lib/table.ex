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

  @type table_info :: %{columns: list(column())}

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
  @spec to_rows(Reader.t(), keyword()) :: Enumerable.t()
  def to_rows(tabular, opts \\ []) do
    tabular |> to_rows_with_info(opts) |> elem(0)
  end

  @doc """
  Same as `to_rows/2`, extended with information about the table.

  ## Examples

      iex> data = %{id: [1, 2, 3], name: ["Sherlock", "John", "Mycroft"]}
      iex> {_rows, info} = Table.to_rows_with_info(data)
      iex> info
      %{columns: [:id, :name]}

  """
  @spec to_rows_with_info(Reader.t(), keyword()) :: {Enumerable.t(), table_info()}
  def to_rows_with_info(tabular, opts \\ []) do
    only = opts[:only] && MapSet.new(opts[:only])

    reader = init_reader!(tabular)
    {read_rows(reader, only), get_info(reader)}
  end

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
    Table.Zipper.zip_with(enum, fn values ->
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
  @spec to_columns(Reader.t(), keyword()) :: %{column() => Enumerable.t()}
  def to_columns(tabular, opts \\ []) do
    tabular |> to_columns_with_info(opts) |> elem(0)
  end

  @doc """
  Same as `to_columns/2`, extended with information about the table.

  ## Examples

      iex> data = [%{id: 1, name: "Sherlock"}, %{id: 2, name: "John"}, %{id: 3, name: "Mycroft"}]
      iex> {_columns, info} = Table.to_columns_with_info(data)
      iex> info
      %{columns: [:id, :name]}

  """
  @spec to_columns_with_info(Reader.t(), keyword()) ::
          {%{column() => Enumerable.t()}, table_info()}
  def to_columns_with_info(tabular, opts \\ []) do
    only = opts[:only] && MapSet.new(opts[:only])

    reader = init_reader!(tabular)
    {read_columns(reader, only), get_info(reader)}
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

  defp get_info({_, %{columns: columns}, _}), do: %{columns: columns}
end
