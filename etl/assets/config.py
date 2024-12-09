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
                "tblMachineProduction",
                "tblRewinderProduction",
                "tblWrapperProduction",
            ],
            "storage_path_prefix": "bronze/db/production",
        },
        "tracker": {
            "db_uri": os.environ["DB_TRACKER_URI"],
            "tables": [
              # "tblPulperMix"
            ],
            "storage_path_prefix": "bronze/db/tracker",
        },
        "OBCC": {
            "db_uri": os.environ["DB_OBCC_URI"],
            "tables": [
              # "winderDown"
            ],
            "storage_path_prefix": "bronze/db/OBCC",
        },
        "STTissueProductionQRT": {
            "db_uri": os.environ["DB_STTISSUEPRODUCTIONQRT_URI"],
            "tables": [
              # "masterData"
            ],
            "storage_path_prefix": "bronze/db/STTissueProductionQRT",
        },
        "ctc_custom": {
            "db_uri": os.environ["DB_CTC_CUSTOM_URI"],
            "tables": [
              # "ctc_Chem_Data"
            ],
            "storage_path_prefix": "bronze/db/ctc_custom",
        },
        "ctc_config": {
            "db_uri": os.environ["DB_CTC_CONFIG_URI"],
            "tables": [
              # "ctc_tag"
            ],
            "storage_path_prefix": "bronze/db/ctc_config",
        },
    },
    "silver_path_prefix": "silver",
    "warehouse_uri": os.environ["DB_WAREHOUSE_URI"],
}