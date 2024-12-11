from dagster import AssetIn, file_relative_path
from dagstermill import define_dagstermill_asset


validation_notebook = define_dagstermill_asset(
    name="validation_notebook",
    notebook_path=file_relative_path(__file__, "../../notebooks/enterprise_search.ipynb"),
    ins={
      "fact_scheduled_loads": AssetIn("fact_scheduled_loads"),
      "fact_inv_not_shipped": AssetIn("fact_inv_not_shipped"),
      "fact_po_production": AssetIn("fact_po_production"),
    },
    group_name="validation",
)
