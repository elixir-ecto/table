defmodule Table.Zipper do
  @moduledoc false

  # An enumerable that zips several enumerables.
  #
  # This enumerable proxies traversal to the underlying enumerables,
  # so it keeps the same properties, such as optimized slicing.

  defstruct [:enumerables, :fun]

  @doc """
  Returns an enumerable that zips corresponding elements from a
  collection of enumerables into a tuple.
  """
  @spec zip(list(Enumerable.t())) :: Enumerable.t()
  def zip(enumerables) when is_list(enumerables) do
    zip_with(enumerables, &List.to_tuple/1)
  end

  @doc """
  Returns an enumerable that zips corresponding elements from a
  collection using the `zip_fun` function.
  """
  @spec zip_with(list(Enumerable.t()), (list() -> term())) :: Enumerable.t()
  def zip_with(enumerables, zip_fun) when is_list(enumerables) do
    %__MODULE__{enumerables: enumerables, fun: zip_fun}
  end

  defimpl Enumerable do
    def count(%{enumerables: []}), do: {:ok, 0}

    def count(zipper) do
      zipper.enumerables
      |> Enum.reduce_while(:infinity, fn enumerable, min_count ->
        case Enumerable.count(enumerable) do
          {:ok, count} -> {:cont, min(count, min_count)}
          _ -> {:halt, nil}
        end
      end)
      |> case do
        nil -> {:error, __MODULE__}
        count -> {:ok, count}
      end
    end

    def member?(_zipper, _element), do: {:error, __MODULE__}

    def reduce(zipper, acc, fun) do
      zipper.enumerables
      |> stream_zip_with(zipper.fun)
      |> Enumerable.reduce(acc, fun)
    end

    def slice(%{enumerables: []}), do: {:ok, 0, fn _start, _length -> [] end}

    def slice(zipper) do
      zipper.enumerables
      |> Enum.reduce_while({[], [], []}, fn enumerable, {sizes, fun2s, fun3s} ->
        case Enumerable.slice(enumerable) do
          {:ok, size, fun} when is_function(fun, 2) ->
            {:cont, {[size | sizes], [fun | fun2s], nil}}

          {:ok, size, fun} when is_function(fun, 3) ->
            {:cont, {[size | sizes], [(&fun.(&1, &2, 1)) | fun2s], fun3s && [fun | fun3s]}}

          _ ->
            {:halt, nil}
        end
      end)
      |> case do
        {sizes, fun2s, nil} ->
          fun = fn start, length ->
            fun2s
            |> Enum.reduce([], fn fun, slices -> [fun.(start, length) | slices] end)
            |> stream_zip_with(zipper.fun)
            |> Enum.to_list()
          end

          {:ok, Enum.min(sizes), fun}

        {sizes, _fun2s, fun3s} ->
          fun = fn start, length, step ->
            fun3s
            |> Enum.reduce([], fn fun, slices -> [fun.(start, length, step) | slices] end)
            |> stream_zip_with(zipper.fun)
            |> Enum.to_list()
          end

          {:ok, Enum.min(sizes), fun}

        nil ->
          {:error, __MODULE__}
      end
    end

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
end
