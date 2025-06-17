-- Table for storing company information
CREATE TABLE company (
    company_number VARCHAR(20) PRIMARY KEY,  -- company_number is the primary key and unique
    name VARCHAR(255),
    jurisdiction_code VARCHAR(10),
    current_status VARCHAR(50),
    company_type VARCHAR(100),
    incorporation_date DATE,
    dissolution_date DATE,
    registry_url VARCHAR(255),
    registered_address VARCHAR(255),
    retrieved_at TIMESTAMP
);

-- Table for storing officer information
CREATE TABLE officer (
    company_number VARCHAR(20) REFERENCES company(company_number) ON DELETE CASCADE,  -- Foreign key to company table
    name VARCHAR(255),
    position VARCHAR(100)  -- Officer's position (e.g., agent)
    -- No primary key here
);

-- Table for storing mailing addresses
CREATE TABLE mailing_address (
    company_number VARCHAR(20) REFERENCES company(company_number) ON DELETE CASCADE,  -- Foreign key to company table
    street_address VARCHAR(255),
    locality VARCHAR(100),
    region VARCHAR(100),
    postal_code VARCHAR(20)
    -- No primary key here
);

-- Table for storing headquarters addresses
CREATE TABLE headquarters_address (
    company_number VARCHAR(20) REFERENCES company(company_number) ON DELETE CASCADE,  -- Foreign key to company table
    street_address VARCHAR(255),
    locality VARCHAR(100),
    region VARCHAR(100),
    postal_code VARCHAR(20)
    -- No primary key here
);

-- Table for storing officer addresses
CREATE TABLE officer_address (
    company_number VARCHAR(20) REFERENCES company(company_number) ON DELETE CASCADE,  -- Foreign key to company table
    officer_name VARCHAR(255),  -- Officer's name associated with the address
    street_address VARCHAR(255),
    locality VARCHAR(100),
    region VARCHAR(100),
    postal_code VARCHAR(20)
    -- No primary key here
);

-- Table for storing company identifiers
CREATE TABLE company_identifier (
    company_number VARCHAR(20) REFERENCES company(company_number) ON DELETE CASCADE,  -- Foreign key to company table
    identifier_system_code VARCHAR(50), 
    uid VARCHAR(50)
    -- No primary key here
);

-- Table for storing all attributes with specific columns for each key in JSON
CREATE TABLE all_attributes (
    company_number VARCHAR(20) REFERENCES company(company_number) ON DELETE CASCADE,  -- Foreign key to company table
    jurisdiction_of_origin VARCHAR(50),
    registry_type VARCHAR(50), 
    presence_of_charges VARCHAR(5),
    is_in_liquidation VARCHAR(5),
    receiver_appointed VARCHAR(5),
    documents VARCHAR(255) 
    liquidator JSONB,
    receiver JSONB,
    -- No primary key here
);


-- {"jurisdiction_code":"im","registry_url":"https://services.gov.im/ded/services/companiesregistry/viewcompany.iom?Id=LEG2U53uSWpQr0NkQgYMqw%3d%3d","retrieved_at":"2024-10-03T23:24:14Z","all_attributes":{"Registry Type":"Business Name","Place of Business":"3 REAYRT NY GLIONNEY CHASE, LAXEY, IM4 7LF, Isle of Man","Presence of Charges":"No","Is in Liquidation?":"No","Receiver(s) Appointed?":"No","Documents":"2 public documents available"},"previous_names":[{"company_name":"Home and Tone","other_attributes":{"status":"Current"}}],"name":"Home and Tone","company_number":"030752B","incorporation_date":"2024-10-03","current_status":"Live","registered_address":"3 REAYRT NY GLIONNEY CHASE, LAXEY, IM4 7LF, Isle of Man"}


-- Table for storing previous company names
CREATE TABLE previous_names (
    company_number VARCHAR(20) REFERENCES company(company_number) ON DELETE CASCADE,  -- Foreign key to company table
    previous_name VARCHAR(255), 
    status VARCHAR(50) 
    -- No primary key here
);