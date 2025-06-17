-- Table for storing company information
CREATE TABLE stg_us_fl_company (
    company_number VARCHAR(20) PRIMARY KEY
    name VARCHAR(255),
    jurisdiction_code VARCHAR(10),
    current_status VARCHAR(50),
    company_type VARCHAR(100),
    incorporation_date JSONB,
    dissolution_date JSONB,
    registry_url VARCHAR(255),
    officers JSONB,
    registered_address VARCHAR(500),
    mailing_address JSONB,
    headquarters_address JSONB,
    company_identifiers JSONB,
    all_attributes JSONB,
    previous_names JSONB,
    fetched_by VARCHAR(50), -- user or system who fetched the raw data
    fetched_at TIMESTAMP, -- timestamp when the raw data was fetched from the jurisdiction
    parsed_by VARCHAR(50), -- name and version of the config file used to parse the raw data
    parsed_at TIMESTAMP, -- timestamp when the raw data was extracted from raw files (html, csv, json, xml)
    cdm_mapping_rules VARCHAR(50),  -- name and version of the config file used to map the raw data to cdm schema
    cdm_mapped_at TIMESTAMP, -- timestamp when the raw data was mapped to cdm (company) schema  
);  