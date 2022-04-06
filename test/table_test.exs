defmodule TableTest do
  use ExUnit.Case, async: true

  doctest Table

  @row_data [
    %{"id" => 1, "name" => "Sherlock"},
    %{"id" => 2, "name" => "John"}
  ]

  @column_data [
    {"id", [1, 2]},
    {"name", ["Sherlock", "John"]}
  ]

  describe "to_rows/1" do
    test "raises given invalid tabular data" do
      assert_raise ArgumentError, "expected valid tabular data, but got: []", fn ->
        Table.to_rows([])
      end
    end

    test "row data" do
      assert @row_data |> Table.to_rows() |> Enum.to_list() == [
               %{"id" => 1, "name" => "Sherlock"},
               %{"id" => 2, "name" => "John"}
             ]
    end

    test "row data with :only" do
      assert @row_data |> Table.to_rows(only: ["name"]) |> Enum.to_list() == [
               %{"name" => "Sherlock"},
               %{"name" => "John"}
             ]
    end

    test "column data" do
      assert @column_data |> Table.to_rows() |> Enum.to_list() == [
               %{"id" => 1, "name" => "Sherlock"},
               %{"id" => 2, "name" => "John"}
             ]
    end

    test "column data with :only" do
      assert @column_data |> Table.to_rows(only: ["name"]) |> Enum.to_list() == [
               %{"name" => "Sherlock"},
               %{"name" => "John"}
             ]
    end
  end

  describe "to_columns/1" do
    test "raises given invalid tabular data" do
      assert_raise ArgumentError, "expected valid tabular data, but got: []", fn ->
        Table.to_columns([])
      end
    end

    test "row data" do
      assert @row_data |> Table.to_columns() |> enumerate_columns() == %{
               "id" => [1, 2],
               "name" => ["Sherlock", "John"]
             }
    end

    test "row data with :only" do
      assert @row_data |> Table.to_columns(only: ["name"]) |> enumerate_columns() == %{
               "name" => ["Sherlock", "John"]
             }
    end

    test "column data" do
      assert @column_data |> Table.to_columns() |> enumerate_columns() == %{
               "id" => [1, 2],
               "name" => ["Sherlock", "John"]
             }
    end

    test "column data with :only" do
      assert @column_data |> Table.to_columns(only: ["name"]) |> enumerate_columns() == %{
               "name" => ["Sherlock", "John"]
             }
    end
  end

  defp enumerate_columns(%{} = columns) do
    Map.new(columns, fn {column, values} -> {column, Enum.to_list(values)} end)
  end
end
