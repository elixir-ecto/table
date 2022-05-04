defmodule Table.MixProject do
  use Mix.Project

  @version "0.1.1"
  @description "Unified access to tabular data"

  def project do
    [
      app: :table,
      version: @version,
      description: @description,
      name: "Table",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package()
    ]
  end

  def application do
    []
  end

  defp deps do
    [
      {:ex_doc, "~> 0.28", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "Table",
      source_url: "https://github.com/dashbitco/table",
      source_ref: "v#{@version}"
    ]
  end

  def package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/dashbitco/table"
      }
    ]
  end
end
