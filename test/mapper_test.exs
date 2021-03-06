defmodule Table.MapperTest do
  use ExUnit.Case, async: true

  alias Table.Mapper

  test "count" do
    enumerable = 1..3 |> Mapper.map(fn x -> x * x end)
    assert Enum.count(enumerable) == 3

    enumerable = [1, 2, 3] |> Mapper.map(fn x -> x * x end)
    assert Enum.count(enumerable) == 3
  end

  test "reduce" do
    enumerable = 1..3 |> Mapper.map(fn x -> x * x end)
    assert Enum.reduce(enumerable, &(&1 + &2)) == 14
  end

  test "slice" do
    enumerable = 1..10 |> Mapper.map(fn x -> x * x end)
    assert Enum.slice(enumerable, 4..6) == [25, 36, 49]

    enumerable = 1..10 |> Stream.map(& &1) |> Mapper.map(fn x -> x * x end)
    assert Enum.slice(enumerable, 4..6) == [25, 36, 49]
  end

  test "member?" do
    enumerable = 1..10 |> Mapper.map(fn x -> x * x end)
    assert Enum.member?(enumerable, 36) == true
    assert Enum.member?(enumerable, 37) == false
  end

  test "mapping multiple times retains a flat mapper" do
    enumerable =
      1..3
      |> Mapper.map(fn x -> x * x end)
      |> Mapper.map(fn x -> x + 1 end)

    assert %Mapper{enumerable: 1..3} = enumerable

    assert Enum.to_list(enumerable) == [2, 5, 10]
  end
end
