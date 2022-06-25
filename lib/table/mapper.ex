defmodule Table.Mapper do
  @moduledoc false

  # An enumerable that maps a function over another enumerable.
  #
  # This enumerable proxies traversal to the underlying enumerable,
  # so it keeps the same properties, such as optimized slicing.

  defstruct [:enumerable, :mapper]

  @doc """
  Returns an enumerable that will apply the given function on
  enumeration.
  """
  @spec map(Enumerable.t(), (any() -> any())) :: Enumerable.t()
  def map(enumerable, fun) do
    %__MODULE__{enumerable: enumerable, mapper: fun}
  end

  defimpl Enumerable do
    def count(proxy) do
      Enumerable.count(proxy.enumerable)
    end

    def member?(proxy, element) do
      # The mapping is not necessarily reversible, so we fall
      # back to a linear search. For enumerables representing
      # data entries member? would generally be linear anyway
      Enum.any?(proxy.enumerable, fn original ->
        proxy.mapper.(original) == element
      end)
    end

    def reduce(proxy, acc, fun) do
      Enumerable.reduce(proxy.enumerable, acc, fn original, acc ->
        fun.(proxy.mapper.(original), acc)
      end)
    end

    def slice(proxy) do
      case Enumerable.slice(proxy.enumerable) do
        {:ok, size, fun} ->
          fun =
            case fun do
              to_list_fun when is_function(to_list_fun, 1) ->
                &(to_list_fun.(&1) |> Enum.map(proxy.mapper))

              slicing_fun when is_function(slicing_fun, 2) ->
                &(slicing_fun.(&1, &2) |> Enum.map(proxy.mapper))

              slicing_fun when is_function(slicing_fun, 3) ->
                &(slicing_fun.(&1, &2, &3) |> Enum.map(proxy.mapper))
            end

          {:ok, size, fun}

        {:error, _module} ->
          {:error, __MODULE__}
      end
    end
  end
end
