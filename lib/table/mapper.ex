defmodule Table.Mapper do
  @moduledoc false

  # An enumerable that maps a function over another enumerable.
  #
  # This enumerable proxies traversal to the underlying enumerable,
  # so it keeps the same properties, such as optimized slicing.

  defstruct [:enumerable, :fun]

  @doc """
  Returns an enumerable that will apply the given function on
  enumeration.
  """
  @spec map(Enumerable.t(), (any() -> any())) :: Enumerable.t()
  def map(enumerable, fun)

  def map(%__MODULE__{} = mapper, fun) do
    %{mapper | fun: &fun.(mapper.fun.(&1))}
  end

  def map(enumerable, fun) do
    %__MODULE__{enumerable: enumerable, fun: fun}
  end

  defimpl Enumerable do
    def count(mapper) do
      with {:error, _} <- Enumerable.count(mapper.enumerable) do
        {:error, __MODULE__}
      end
    end

    # The mapping is not necessarily reversible, so we fall
    # back to a linear search. For enumerables representing
    # data entries member? would generally be linear anyway
    def member?(_mapper, _element), do: {:error, __MODULE__}

    def reduce(mapper, acc, fun) do
      Enumerable.reduce(mapper.enumerable, acc, fn original, acc ->
        fun.(mapper.fun.(original), acc)
      end)
    end

    def slice(mapper) do
      case Enumerable.slice(mapper.enumerable) do
        {:ok, size, fun} ->
          fun =
            case fun do
              to_list_fun when is_function(to_list_fun, 1) ->
                &(to_list_fun.(&1) |> Enum.map(mapper.fun))

              slicing_fun when is_function(slicing_fun, 2) ->
                &(slicing_fun.(&1, &2) |> Enum.map(mapper.fun))

              slicing_fun when is_function(slicing_fun, 3) ->
                &(slicing_fun.(&1, &2, &3) |> Enum.map(mapper.fun))
            end

          {:ok, size, fun}

        {:error, _module} ->
          {:error, __MODULE__}
      end
    end
  end
end
