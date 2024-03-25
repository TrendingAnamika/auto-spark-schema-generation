This code is developed to automatically generate a Spark Schema for a given JSON file or data of any structure.

Requirement : Develop a Generic Function which can extract columns and its datatype from any dynamic json and create Schema for the same. This use case is to Handle if Number of Columns and its Datatype got changed at source.

Problem Statement: If we receive JSON from a source that consistently contains the same key-value pairs (data) with the same types, we can create a custom Spark Schema to process it further. However, what if the JSON can have additional fields or the data types change at the source? In such cases, we may encounter data loss. If we generate the schema at runtime from the given JSON (source data) based on its data, there is no chance of data loss or any other issues related to data or data types. Since this code is optimized, it is recommended to use it if you have a similar use case.

Similar schema can also be obtained by inferring schema in Spark, but it can be computationally costly. Therefore, it's better to use the custom schema approach described above!

This project contains below code files :

auto_schema_generation.py => Main Code to Generate Auto Spark Schema and Test on a Spark DataFrame.
auto_schema_generation_test.py => Pytest with Samples of five JSON and Their Expected Schema. All five Test Cases Should Pass.
