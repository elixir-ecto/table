defmodule Table.Reader.Enumerable do
  @moduledoc false

  @doc """
  Tries to initialize column table reader for the given enumerable.
  """
  @spec init_columns(Enumerable.t()) :: Table.Reader.column_reader() | :none
  def init_columns(enum) do
    enum
    |> Enum.reduce_while({:ok, [], []}, fn item, {:ok, columns, data} ->
      with {column, values} <- item, true <- enumerable?(values) do
        {:cont, {:ok, [column | columns], [values | data]}}
      else
        _ -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, [], []} ->
        :none

      {:ok, columns, data} ->
        meta = %{columns: Enum.reverse(columns)}
        {:columns, meta, Enum.reverse(data)}

      :error ->
        :none
    end
  end

  @doc """
  Tries to initialize row table reader for the given enumerable.
  """
  @spec init_rows(Enumerable.t()) :: Table.Reader.row_reader() | :none
  def init_rows(enum) do
    case Enum.fetch(enum, 0) do
      {:ok, head} ->
        case columns_for(head) do
          {:ok, columns} ->
            meta = %{columns: columns}
            enum = Table.Mapper.map(enum, &record_values(&1, columns, head))
            {:rows, meta, enum}

          :error ->
            :none
        end

      :error ->
        {:rows, %{columns: []}, []}
    end
  end

  defp enumerable?(term), do: Enumerable.impl_for(term) != nil

  defp columns_for(%_{} = _struct) do
    :error
  end

  defp columns_for(record) when is_map(record) do
    {:ok, record |> Map.keys() |> Enum.sort()}
  end

  defp columns_for(record) when is_list(record) do
    keyval_columns(record, [])
  end

  defp columns_for(_record), do: :error

  defp keyval_columns([], []), do: :error
  defp keyval_columns([], columns), do: {:ok, Enum.reverse(columns)}
  defp keyval_columns([{key, _} | rest], columns), do: keyval_columns(rest, [key | columns])
  defp keyval_columns(_list, _columns), do: :error

  defp record_values(record, columns, _head_record) when is_list(record) do
    keyval_values(record, columns)
  end

  defp record_values(record, columns, head_record) when is_map(record) do
    {values, remaining_record} =
      Enum.map_reduce(columns, record, fn column, remaining_record ->
        try do
          Map.pop!(remaining_record, column)
        rescue
          KeyError ->
            raise "map records must have the same columns, missing column #{inspect(column)} in #{inspect(record)}"
        end
      end)

    if remaining_record != %{} do
      raise "map records must have the same columns, missing column(s) #{inspect(Map.keys(remaining_record))} in #{inspect(head_record)}"
    else
      values
    end
  end

  defp record_values(record, _head_record, _columns) do
    raise "invalid table record: #{inspect(record)}"
  end

  defp keyval_values([], []), do: []

  defp keyval_values([{column, value} | rest], [column | columns]) do
    [value | keyval_values(rest, columns)]
  end

  defp keyval_values([], [column | _columns]) do
    raise "key-value records must have the same columns, missing #{inspect(column)}"
  end

  defp keyval_values([{column, _value}], []) do
    raise "key-value records must have the same columns, missing #{inspect(column)}"
  end

  defp keyval_values([{actual, _value} | _rest], [column | _columns]) do
    raise "key-value records must have columns in the same order, expected #{inspect(column)}, but got #{inspect(actual)}"
  end

  defp keyval_values([item | _rest], _columns) do
    raise "expected a key-value pair, but got: #{inspect(item)}"
  end
end
