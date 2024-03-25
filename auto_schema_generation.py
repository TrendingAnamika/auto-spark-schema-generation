import json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, ArrayType, MapType, \
    LongType, FloatType, IntegerType, DoubleType

""" 
Requirement : 
    Develop a Generic Function which can extract columns and its datatype from any dynamic json and create Schema for the same.
    This use case is to Handle if Number of Columns and its Datatype got changed at source.
"""

# Develop a function generate_auto_spark_schema with below points :
"""
Parse JSON data to extract column names and data types, then generate a PySpark schema dynamically.
Parameters: json_data (str): JSON data representing a list of dictionaries (rows).
Returns: schema (StructType): PySpark StructType schema object.
"""

def generate_auto_spark_schema(json_data):

    data = json.loads(json_data)        #Parse JSON data
    if isinstance(data, list):          #Check if json is a list of dictionaries or a single dictionary
        first_row = data[0]             #Assuming that the first row contains all columns
    else:
        first_row = data

    #Generate schema
    fields = []
    for column_name, column_data in first_row.items():
        field_type = get_field_type(column_data)
        fields.append(StructField(column_name, field_type, nullable=True))

    schema = StructType(fields)

    return schema

#Function get_field_type() to call in generate_auto_spark_schema :
"""
Determine the PySpark data type for a given field value.
"""
def get_field_type(field_value):

    if isinstance(field_value, dict):
        return get_struct_type(field_value)
    elif isinstance(field_value, list):
        return get_array_type(field_value)
    elif isinstance(field_value, bool):
        return BooleanType()
    elif isinstance(field_value, int):
        return LongType()               #Assuming that all integers are LongType
    elif isinstance(field_value, float):
        return FloatType()
    elif isinstance(field_value, str):
        try:
            _ = json.loads(field_value)
            return StringType()         #String as StringType
        except ValueError:
            return StringType()         #Default to StringType
    else:
        return StringType()             #Default to StringType


#Function get_struct_type() to call in function get_field_type()
"""
Generate StructType schema for nested dictionaries.
"""
def get_struct_type(data):

    fields = []
    for key, value in data.items():
        field_type = get_field_type(value)
        fields.append(StructField(key, field_type, nullable=True))
    return StructType(fields)


#Function get_array_type() to call in function get_field_type()
"""
Determine the type of elements in the array.
"""
def get_array_type(data):

    if data:
        element_type = get_field_type(data[0])
        return ArrayType(element_type)
    else:
        return ArrayType(StringType())  #Default to ArrayType of StringType if empty list


# Example JSON data
json_data = """[{"name": "test_titan_lever", "description": "Revenue Optimizations", "created ts": "2022-11-29T12:49:22.989+0000", "updated ts": "2022-11-29T12:52:54.843+0000", "tags": {"deployment_request_id": "f3c0f065-8788-4a00-814e-a9271b557366", "provision_request_id": "15fdc745-d9dd-4783-8e7f-64b242a98b0d", "metadata_version": "v1.0", "deployment_status": "DEPLOYED", "create_time": "2022-11-29T12:49:22.989+0000", "update_time": "2022-11-29T12:52:54.843+0000", "island": "supplypartner", "pci_category": "cts", "brand": "eg", "domain": "supplypartner", "attribute_set": "Revenue Optimizations", "dataset": "brain_titan_lever", "schema_type": "ENTITY", "data_store_type": "ELASTIC_SEARCH", "entity_id": "vrbopropertyid", "additional_entities": ["oppyname", "processingdate", "processinghour"], "filter_keys": [], "date_keys": [], "aggregate_keys": [], "primitive_type_transformers": [], "primitive_type_transformer_failure_action": "Skip", "use_case_info": {"dataset_description": "Oppys dataset - Titan Lever", "consumers": [{"client_id": "a62dabc3-0812-4b85-8bf0-9e9051d392e4", "client_name": null, "team_dl": "test-partner-recommendations-team-all@test.com"}]}, "owner_info": {"name": "customer_name", "email": "test_email@test.com", "team_dl": "team-all@test.com", "team_name": "Partner Recommendations Team All", "application_name": "brain-vrregistrationid", "support_slack_channel": "lodg-brain-opex", "security_groups": null}, "capabilities": [{"capability_name": "API", "connector_name": "SD_CONNECTOR", "status": "ENABLED", "request_id": "57dd5048-7a8d-4fbb-9082-7f2e09627a7e", "comments": null, "polling_attempts": 2}, {"capability_name": "API", "connector_name": "ES_CONNECTOR", "status": "ENABLED", "request_id": null, "comments": null, "polling_attempts": 0}], "source": {"stream": null, "hive": {"schema": null, "database_name": "stg_supply", "table_name": "brain_titan_lever_p", "partition": null, "server_name": "egdl-hive-server2.egdp-test.aws.away.black", "environment": "egdp-test", "aws_vpc_id": "vpc-0618333437f727d62"}}, "source_type": "HIVE_SOURCE", "attributes": [{"description": "vrbopropertyid", "name": "vrbopropertyid", "path": "vrbopropertyid", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "oppyid", "name": "oppyid", "path": "oppyid", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "oppycreationtimestamp", "name": "oppycreationtimestamp", "path": "oppycreationtimestamp", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "closuredate", "name": "closuredate", "path": "closuredate", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "isactive", "name": "isactive", "path": "isactive", "type": "boolean", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "category", "name": "category", "path": "category", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "oppydetails", "name": "oppydetails", "path": "oppydetails", "type": "record", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "metadata", "name": "metadata", "path": "metadata", "type": "record", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "type", "name": "type", "path": "type", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "version", "name": "version", "path": "version", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "oppyname", "name": "oppyname", "path": "oppyname", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "processingdate", "name": "processingdate", "path": "processingdate", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}, {"description": "processinghour", "name": "processinghour", "path": "processinghour", "type": "string", "storage_type": null, "additional_source_type_info": null, "transform_operations": []}], "sinks": {"es_sink": {"index_prefix": "eg-attribute-sdk", "index_name": "eg-attribute-sdk_supplypartner_revenueoptimizations_eg_brain_titan_lever_v1_0", "index_name_pattern": null, "index_template_name": null, "index_lifecycle_name": null, "index_lifecycle_rollover_alias": null, "storage_config": {"shards": "1", "replicas": "2", "index_refresh_interval": "30s"}, "cluster_name": "default"}, "scylla_sink": null}, "ttl_in_minutes": 0, "data_volume": {"stream": null, "hive": null}, "governance_tags": null, "soft_delete_window_in_days": 15, "hard_delete_window_in_days": 15, "soft_delete_date": null, "hard_delete_date": null, "dev_request": false, "data_versioning_enabled": false, "data_versioning_info": null, "model_output": false, "model_info": {"model_name": null, "model_version": null}, "notification_info": {"all_info": {"slack_channels": ["lodg-brain-opex"], "emails": ["customer_name@test.com"]}, "error_info": {"slack_channels": ["lodg-brain-opex"], "emails": ["customer_name@test.com"]}}, "data_flatteners": null, "is_non_attribute_store_dataset": false, "domain_key_info": null, "keychain_context": null}}]"""
#json_data = """[{"name":"lever_test","description":"Optimizations Test","created_ts":"2024-03-10T12:49:22.989+0000","updated_ts":"2024-03-10T12:52:54.843+0000","tags":{"test_request_id":"f3a9271b557366","provision_request_id":"157f64b242a98b0d","metadata_version":"v1.00","deployment_status":"DEPLOYED","create_time":"2024-03-10T12:49:22.989+0000","update_time":"2024-03-12T12:52:54.843+0000","island":"supply_partner","test_category":"test","brand":"test","domain":"supply_partner","attribute_set":"Revenue Optimizations","dataset":"lever_test","schema_type":"ENTITY","data_store_type":"ELASTIC_SEARCH","entity_id":"vr_property_id","additional_entities":["ops_name","processing_date","processing_hour"],"filter_keys":[],"date_keys":[],"aggregate_keys":[],"primitive_type_transformers":[],"transformer_failure_action":"Skip","use_case_info":{"dataset_description":"Ops dataset - Lever Test","consumers":[{"client_id":"a62dabc3-8bf09e9051d392e4","client_name":null,"team_dl":"team-all@test.com"}]},"owner_info":{"name":"Anamika Singh","email":"anamika.singh@test.com","team_dl":"team-all@test.com","team_name":"Partner Team All","application_name":"test-registration-id","support_slack_channel":"test_support_channel","security_groups":null}},"capabilities":[{"capability_name":"API","connector_name":"TEST_SD_CONNECTOR","status":"ENABLED","request_id":"9082-7f2e09627a7e","comments":null,"polling_attempts":2},{"capability_name":"TEST API","connector_name":"TEST_ES_CONNECTOR","status":"ENABLED","request_id":null,"comments":null,"polling_attempts":0}],"source":{"stream":null,"hive":{"schema":null,"database_name":"stg_test","table_name":"test_table","partition":null,"server_name":"test.aws.away.black","environment":"dp-test","aws_vpc_id":"vpc-7d62"}},"source_type":"HIVE_SOURCE","attributes":[{"description":"property_id","name":"property_id","path":"property_id","type":"string","storage_type":null,"additional_source_type_info":null,"transform_operations":[]},{"description":"test id","name":"ops id","path":"test_id","type":"string","storage_type":null,"additional_source_type_info":null,"transform_operations":[]},{"description":"test_creation_timestamp","name":"ops_creation_timestamp","path":"ops_creation_timestamp","type":"string","storage_type":null,"additional_source_type_info":null,"transform_operations":[]},{"description":"closure_date","name":"closure_date","path":"closure_date","type":"string","storage_type":null,"additional_source_type_info":null,"transform_operations":[]},{"description":"is_active","name":"is_active","path":"is_active","type":"boolean","storage_type":null,"additional_source_type_info":null,"transform_operations":[]},{"description":"category","name":"category","path":"category","type":"string","storage_type":null,"additional_source_type_info":null,"transform_operations":[]},{"description":"ops_details","name":"ops_details","path":"ops_details","type":"record","storage_type":null,"additional_source_type_info":null,"transform_operations":[]},{"description":"metadata","name":"metadata","path":"metadata","type":"record","storage_type":null,"additional_source_type_info":null}],"sinks":{"es_sink":{"index_prefix":"test-attribute-sdk","index_name":"test_attribute_supply_partner_revenue_optimizations_test_v1_0","index_name_pattern":null,"index_template_name":null,"index_lifecycle_name":null,"index_lifecycle_rollover_alias":null,"storage_config":{"shards":"1","replicas":"3","index_refresh_interval":"20s"},"cluster_name":"default"},"test_sink":null},"ttl_in_minutes":0,"data_volume":{"stream":null,"hive":null},"governance_tags":null,"soft_delete_window_in_days":30,"hard_delete_window_in_days":30,"soft_delete_date":null,"hard_delete_date":null,"dev_request":false,"data_versioning_enabled":false,"data_versioning_info":null,"model_output":false,"model_info":{"model_name":null,"model_version":null},"notification_info":{"all_info":{"slack_channels":["test_channel"],"emails":["anamika.singh@test.com"]},"error_info":{"slack_channels":["test_channel"],"emails":["anamika.singh@test.com"]}},"data_flatteners":null,"is_non_attribute_store_dataset":false,"domain_key_info":null,"keychain_context":null}]"""


#Generate schema
auto_schema = generate_auto_spark_schema(json_data)

#Print schema
print(auto_schema)


#Testing with the above generated schema "auto_schema"

#Creating SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("generate-auto-spark-schema") \
        .getOrCreate()


#Read Json Data using above generated schema
df = spark.read.schema(auto_schema).json(spark.sparkContext.parallelize([json_data]))
df.show()    #Show the DataFrame to verify.

