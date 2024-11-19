import os

config = {
    "ingestion_sources": {
        "production": {
            "db_uri": os.environ["DB_PRODUCTION_CONN_STR"],
            "tables": [
                "tblBillofLadingDetail",
                "tblBillofLadingHeader",
                "tblPurchaseOrder",
                "tblPurchaseOrderDetail",
                "tlkpCustomer",
            ],
            "storage_path_prefix": "bronze/db/production",
        },
        "tracker": {
            "db_uri": os.environ["DB_TRACKER_CONN_STR"],
            "tables": ["tblPulperMix"],
            "storage_path_prefix": "bronze/db/tracker",
        },
    },
    "silver_path_prefix": "silver",
    "warehouse_uri": os.environ["DB_WAREHOUSE_CONN_STR"],
}