defmodule Table.ReaderTest do
  use ExUnit.Case, async: true

  describe "list init/1" do
    test "returns :none for non-tabular lists" do
      assert Table.Reader.init([]) == :none
      assert Table.Reader.init([1, 2, 3]) == :none

      assert Table.Reader.init([[]]) == :none
      assert Table.Reader.init([["a"]]) == :none

      assert Table.Reader.init([{"a", [1, 2], [1, 2]}]) == :none
      assert Table.Reader.init([{"a", [1, 2]}, {"b", 2}]) == :none
    end

    test "list of key-vals" do
      data = [
        [{"id", 1}, {"name", "Sherlock"}],
        [{"id", 2}, {"name", "John"}]
      ]

      assert {:rows, %{columns: ["id", "name"]}, enum} = Table.Reader.init(data)
      assert Enum.to_list(enum) == [[1, "Sherlock"], [2, "John"]]
    end

    test "list of maps" do
      data = [
        %{"id" => 1, "name" => "Sherlock"},
        %{"id" => 2, "name" => "John"}
      ]

      assert {:rows, %{columns: ["id", "name"]}, enum} = Table.Reader.init(data)
      assert Enum.to_list(enum) == [[1, "Sherlock"], [2, "John"]]
    end

    test "list with list series" do
      data = [
        {"id", [1, 2]},
        {"name", ["Sherlock", "John"]}
      ]

      assert {:columns, %{columns: ["id", "name"]}, enum} = Table.Reader.init(data)
      assert [ids, names] = Enum.to_list(enum)
      assert Enum.to_list(ids) == [1, 2]
      assert Enum.to_list(names) == ["Sherlock", "John"]
    end

    test "list with enumerable series" do
      data = [
        {"id", 1..2},
        {"name", Stream.map(["Sherlock", "John"], & &1)}
      ]

      assert {:columns, %{columns: ["id", "name"]}, enum} = Table.Reader.init(data)
      assert [ids, names] = Enum.to_list(enum)
      assert Enum.to_list(ids) == [1, 2]
      assert Enum.to_list(names) == ["Sherlock", "John"]
    end

    test "enumerating rows raises on invalid element" do
      assert {:rows, %{}, enum} = Table.Reader.init([%{a: 1}, 1])

      assert_raise RuntimeError, "invalid table record: 1", fn ->
        Enum.to_list(enum)
      end
    end

    test "enumerating rows raises on missing map element" do
      assert {:rows, %{}, enum} =
               Table.Reader.init([
                 %{"x" => 1, "y" => 1},
                 %{"x" => 2}
               ])

      assert_raise RuntimeError,
                   ~s/map records must have the same columns, missing column "y" in %{"x" => 2}/,
                   fn ->
                     Enum.to_list(enum)
                   end
    end

    test "enumerating rows raises on keyval order mismatch" do
      assert {:rows, %{}, enum} =
               Table.Reader.init([
                 [{"x", 1}, {"y", 1}],
                 [{"y", 2}, {"x", 2}]
               ])

      assert_raise RuntimeError,
                   ~s/key-value records must have columns in the same order, expected "x", but got "y"/,
                   fn ->
                     Enum.to_list(enum)
                   end
    end

    test "enumerating rows raises on invalid keyval element" do
      assert {:rows, %{}, enum} =
               Table.Reader.init([
                 [{"x", 1}, {"y", 1}],
                 [{"x", 2}, 2]
               ])

      assert_raise RuntimeError, ~s/expected a key-value pair, but got: 2/, fn ->
        Enum.to_list(enum)
      end
    end
  end

  describe "map init/1" do
    test "returns :none for non-tabular maps" do
      assert Table.Reader.init(%{}) == :none
      assert Table.Reader.init(%{a: [1, 2], b: 2}) == :none
    end

    test "map with series" do
      data = %{
        "id" => [1, 2],
        "name" => ["Sherlock", "John"]
      }

      assert {:columns, %{columns: ["id", "name"]}, enum} = Table.Reader.init(data)
      assert [ids, names] = Enum.to_list(enum)
      assert Enum.to_list(ids) == [1, 2]
      assert Enum.to_list(names) == ["Sherlock", "John"]
    end
  end
end
