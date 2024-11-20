import os

config = {
    "ingestion_sources": {
        "production": {
            "db_uri": os.environ["DB_PRODUCTION_URI"],
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
            "db_uri": os.environ["DB_TRACKER_URI"],
            "tables": ["tblPulperMix"],
            "storage_path_prefix": "bronze/db/tracker",
        },
        "ctc_custom": {
            "db_uri": os.environ["DB_CTC_CUSTOM_URI"],
            "tables": ["ctc_Chem_Data"],
            "storage_path_prefix": "bronze/db/ctc_custom",
        },
    },
    "silver_path_prefix": "silver",
    "warehouse_uri": os.environ["DB_WAREHOUSE_URI"],
}