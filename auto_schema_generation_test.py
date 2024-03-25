import pytest
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, ArrayType, LongType

# Import the functions from the source code
from auto_schema_generation import generate_auto_spark_schema

@pytest.mark.parametrize("json_data, expected_schema", [
    ("""
    [
  {
    "name": "titan_test",
    "description": "Optimizations Test",
    "tags": {
      "metadata_version": "v1.0",
      "brand": "test",
      "domain": "supply_test",
      "additional_entities": ["test_name", "processing_date"],
      "filter_keys": [],
      "use_case_info": {
        "dataset_description": "Test dataset",
        "consumers": [
          {
            "client_id": "test_12345",
            "client_name": null,
            "test_dl": "test@test.com"
          }
        ]
      },
      "owner_info": {
        "name": "Anamika Singh",
        "email": "anamika.singh@test.com"
      }
    }
  }
]
    """,
     StructType([
         StructField('name', StringType(), True),
         StructField('description', StringType(), True),
         StructField('tags', StructType([
             StructField('metadata_version', StringType(), True),
             StructField('brand', StringType(), True),
             StructField('domain', StringType(), True),
             StructField('additional_entities', ArrayType(StringType(), True), True),
             StructField('filter_keys', ArrayType(StringType(), True), True),
             StructField('use_case_info', StructType([
                 StructField('dataset_description', StringType(), True),
                 StructField('consumers', ArrayType(StructType([
                      StructField('client_id', StringType(), True),
                      StructField('client_name', StringType(), True),
                      StructField('test_dl', StringType(), True)]), True), True)]), True),
             StructField('owner_info', StructType([
                 StructField('name', StringType(), True),
                 StructField('email', StringType(), True)]), True)]), True)])
    ),
    ("""
    {
    "name": "lever test",
    "description": "Revenue Test",
    "tags": {
      "metadata_version": "v1.2",
      "brand": "test",
      "use_case_info": {
        "dataset_description": "Test dataset",
        "consumers": [
          {
            "client_id": "9edftrey"
          }
        ]
      },
      "owner_info": {
        "name": "Anamika Singh",
        "email": "anamika.singh@test.com"
      }
    }
  }
""",
     StructType([
         StructField('name', StringType(), True),
         StructField('description', StringType(), True),
         StructField('tags', StructType([
             StructField('metadata_version', StringType(), True),
             StructField('brand', StringType(), True),
             StructField('use_case_info', StructType([
                 StructField('dataset_description', StringType(), True),
                 StructField('consumers', ArrayType(StructType([
                     StructField('client_id', StringType(), True)]), True), True)]), True),
             StructField('owner_info', StructType([
                 StructField('name', StringType(), True),
                 StructField('email', StringType(), True)]), True)]), True)])
    ),

    ("""[
  {
    "name": "lever_info",
    "description": "Best Optimizations",
      "owner_info": {
        "name": "Anamika Singh",
        "email": "anamika.singh@test.com"
      }
  }
]""",
      StructType([
          StructField('name', StringType(), True),
          StructField('description', StringType(), True),
          StructField('owner_info', StructType([
              StructField('name', StringType(), True),
              StructField('email', StringType(), True)]), True)])
    ),
    ("""
  {
    "id": 1234,
    "is_description": true,
    "name": "Anamika Singh",
    "email": "anamika.singh@test.com"
  }
""",
     StructType([
                 StructField('id', LongType(), True),
                 StructField('is_description', BooleanType(), True),
                 StructField('name', StringType(), True),
                 StructField('email', StringType(), True)])
     ),
    ("""
  {
    "module": "test",
    "description": "test",
    "id": 12345
  }
""",
     StructType([
         StructField('module', StringType(), True),
         StructField('description', StringType(), True),
         StructField('id', LongType(), True)])
     )
])


def test_generate_auto_spark_schema(json_data, expected_schema):
    schema = generate_auto_spark_schema(json_data)
    assert schema == expected_schema










