defmodule Table.ZipperTest do
  use ExUnit.Case, async: true

  alias Table.Zipper

  test "count" do
    enumerable = Zipper.zip([1..4, 1..3, 1..5])
    assert Enum.count(enumerable) == 3

    enumerable = Zipper.zip([1..4, [1, 2, 3], 1..5])
    assert Enum.count(enumerable) == 3
  end

  test "reduce" do
    enumerable = Zipper.zip([1..4, 1..3, 1..5])
    assert Enum.reduce(enumerable, [], &[&1 | &2]) == [{3, 3, 3}, {2, 2, 2}, {1, 1, 1}]
  end

  test "slice" do
    enumerable = Zipper.zip([1..7, 1..6, 1..5])
    assert Enum.slice(enumerable, 2..3) == [{3, 3, 3}, {4, 4, 4}]

    enumerable = Zipper.zip([[1, 2, 3], Stream.map(1..10, & &1)])
    assert Enum.slice(enumerable, 1..2) == [{2, 2}, {3, 3}]
  end

  test "member?" do
    enumerable = Zipper.zip([1..4, 1..3, 1..5])
    assert Enum.member?(enumerable, {2, 2, 2}) == true
    assert Enum.member?(enumerable, {7, 7, 7}) == false
  end
end
