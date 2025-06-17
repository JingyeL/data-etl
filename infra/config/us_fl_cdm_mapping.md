## US_FL mapping


| cdm_field                                      | source_field               | notes                                                                                  |
|------------------------------------------------|----------------------------|----------------------------------------------------------------------------------------|
| jurisdiction_code                              | "us_fl"                    | All data from this source belongs to the us_fl jurisdiction                            |
| company_number                                 | COR_NUMBER                 |                                                                                        |
| name                                           | COR_NAME                   |                                                                                        |
| current_status                                 | COR_STATUS                 |                                                                                        |
| incorporation_date                             | COR_FILE_DATE              |                                                                                        |
| headquarters_address.street_address            | [COR_PRINC_ADD_1, COR_PRINC_ADD_2] | Join COR_PRINC_ADD_1 and COR_PRINC_ADD_2 together with ", "                             |
| headquarters_address.locality                  | COR_PRINC_CITY             |                                                                                        |
| headquarters_address.region                    | COR_PRINC_STATE            |                                                                                        |
| headquarters_address.postal_code               | COR_PRINC_ZIP              |                                                                                        |
| mailing_address.street_address                 | [COR_MAIL_ADD_1, COR_MAIL_ADD_2]  | Join COR_MAIL_ADD_1 and COR_MAIL_ADD_2 together with ", "                               |
| mailing_address.locality                       | COR_MAIL_CITY              |                                                                                        |
| mailing_address.region                         | COR_MAIL_STATE             |                                                                                        |
| mailing_address.postal_code                    | COR_MAIL_ZIP               |                                                                                        |
| all_attributes.jurisdiction_of_origin          | STATE_COUNTRY              |                                                                                        |
| company_type                                   | COR_FILING_TYPE            |                                                                                        |
| identifiers[0].uid                             | COR_FEI_NUMBER             |                                                                                        |
| identifiers[0].identifier_system_code          | identifiers[0].identifier_system_code | All identifiers in the COR_FEI_NUMBER column belong to the us_fein scheme |
| retrieved_at                                   | RetrievedAt                |                                                                                        |
| officers[0].name                               | RA_NAME                    |                                                                                        |
| officers[0].position                           | officers[0].position       | The name of the officer included in the RA_NAME column is specifically the officer occupying the agent position |
| officers[0].other_attributes.type              | RA_NAME_TYPE               |                                                                                        |
| officers[0].other_attributes.address.street_address | RA_ADD_1              |                                                                                        |
| officers[0].other_attributes.address.locality  | RA_CITY                    |                                                                                        |
| officers[0].other_attributes.address.region    | RA_STATE                   |                                                                                        |
| officers[0].other_attributes.address.postal_code | RA_ZIP5                 | Join RA_ZIP5 and RA_ZIP4 together with "-"                                              |
| officers[N].name                               | PRINC{N}_NAME              |                                                                                        |
| officers[N].position                           | PRINC{N}_TITLE             |                                                                                        |
| officers[N].other_attributes.type              | PRINC{N}_NAME_TYPE         |                                                                                        |
| officers[N].other_attributes.address.street_address | PRINC{N}_ADD_1       |                                                                                        |
| officers[N].other_attributes.address.locality  | PRINC{N}_CITY              |                                                                                        |
| officers[N].other_attributes.address.region    | PRINC{N}_STATE             |                                                                                        |
| officers[N].other_attributes.address.postal_code | PRINC{N}_ZIP5           | Join PRINC{N}_ZIP5 and PRINC{N}_ZIP4 together with "-"                                  |

Note: N is a number from 1 to 6

## cmd company
company_number VARCHAR(20) PRIMARY KEY
name VARCHAR(255),
jurisdiction_code VARCHAR(10),
current_status VARCHAR(50),
company_type VARCHAR(100),
incorporation_date DATE,
dissolution_date DATE,
registry_url VARCHAR(255),
registered_address VARCHAR(255),
officer JSONB,
mailing_address JSONB,
headquarters_address JSONB,
company_identifier JSONB,
all_attributes JSONB,
previous_names JSONB,
source_fetched_at TIMESTAMP, -- timestamp when the raw data was fetched from the jurisdiction
extracted_at TIMESTAMP, -- timestamp when the raw data was extracted from raw files (html, csv, json, xml)
raw_parser VARCHAR(50), -- raw data parser name and version
cdm_mapper VARCHAR(50), -- cdm (company) schema transformer name and version
retrieved_at TIMESTAMP