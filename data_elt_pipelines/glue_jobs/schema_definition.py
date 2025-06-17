from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# table definition
# CREATE TABLE staging.stg_us_fl_company (
#     company_number VARCHAR(20),
#     name VARCHAR(255),
#     jurisdiction_code VARCHAR(10),
#     current_status VARCHAR(50),
#     company_type VARCHAR(100),
#     incorporation_date JSONB,
#     dissolution_date JSONB,
#     registry_url VARCHAR(255),
#     officers JSONB,
#     registered_address jsonb,
#     mailing_address JSONB,
#     headquarters_address JSONB,
#     company_identifiers JSONB,
#     all_attributes JSONB,
#     previous_names JSONB,
#     periodicity VARCHAR(50),
#     fetched_by VARCHAR(50),
#     fetched_at TIMESTAMP, 
#     parsed_by VARCHAR(50), 
#     parsed_at TIMESTAMP, 
#     cdm_mapping_rules VARCHAR(50),  
#     cdm_mapped_by VARCHAR(50), 
#     cdm_mapped_at TIMESTAMP,
#     source_name VARCHAR(50)
# ); 

cdm_schema = StructType([
    StructField("company_number", StringType(), True),
    StructField("name", StringType(), True),
    StructField("jurisdiction_code", StringType(), True),
    StructField("current_status", StringType(), True),
    StructField("company_type", StringType(), True),
    StructField("incorporation_date", StringType(), True),  #JSONB is stored as a string
    StructField("dissolution_date", StringType(), True),    
    StructField("registry_url", StringType(), True),
    StructField("officers", StringType(), True),            
    StructField("registered_address", StringType(), True),  
    StructField("mailing_address", StringType(), True),     
    StructField("headquarters_address", StringType(), True),
    StructField("company_identifiers", StringType(), True), 
    StructField("all_attributes", StringType(), True),      
    StructField("previous_names", StringType(), True),      
    StructField("periodicity", StringType(), True),
    StructField("fetched_by", StringType(), True),
    StructField("fetched_at", TimestampType(), True),
    StructField("parsed_by", StringType(), True),
    StructField("parsed_at", TimestampType(), True),
    StructField("cdm_mapping_rules", StringType(), True),
    StructField("cdm_mapped_by", StringType(), True),
    StructField("cdm_mapped_at", TimestampType(), True),
    StructField("source_name", StringType(), True)
])

def get_timestamp_fields(schema: StructType) -> list[str]:
    """
    Get timestamp fields
    :param schema: schema
    :return: timestamp fields
    """
    return [field.name for field in schema.fields if field.dataType == TimestampType()]
