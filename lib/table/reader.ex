defprotocol Table.Reader do
  @moduledoc """
  A protocol to read tabular data.
  """

  @typedoc """
  Describes row-based traversal.

  The enumerable should yield rows, where each row is a series of
  values. The values should follow the order of columns in the
  metadata.
  """
  @type row_reader :: {:rows, metadata(), Enumerable.t()}

  @typedoc """
  Describes column-based traversal.

  The enumerable should yield columns, where each column is a series
  of values. The columns should have the same order as columns in the
  metadata.
  """
  @type column_reader :: {:columns, metadata(), Enumerable.t()}

  @typedoc """
  Table metadata.

  User-specific metadata can be added in the form of namespaced
  tuples as keys. The first element is the namespace key, typically an
  atom, and the second is any term. The value can also be anything.
  """
  @type metadata :: %{
          required(:columns) => list(Table.column()),
          optional(:count) => non_neg_integer(),
          optional({term(), term()}) => any()
        }

  @doc """
  Returns information on how to traverse the given tabular data.

  There are generally two distinct ways of traversing tabular data,
  that is, a row-based one and a column-based one. Depending on the
  underlying data representation, one of them is more natural and
  more efficient.

  The `init/1` return value describes either of the two traversal
  types, see `t:row_reader/0` and `t:column_reader/0` respectively.

  Some structs may be tabular only in a subset of cases, therefore
  `:none` may be returned to indicate that there is no valid data to
  read.

  The `init/1` function should not initiate any form of traversal
  or open existing resources. If any setup is necessary, it should
  be peformed when the underlying row or column enumerables are
  traversed.
  """
  @spec init(t()) :: row_reader() | column_reader() | :none
  def init(tabular)
end

defimpl Table.Reader, for: List do
  def init(list) do
    with :none <- Table.Reader.Enumerable.init_columns(list),
         :none <- Table.Reader.Enumerable.init_rows(list),
         do: :none
  end
end

defimpl Table.Reader, for: Map do
  def init(map) do
    Table.Reader.Enumerable.init_columns(map)
  end
end
