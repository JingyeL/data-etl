character-- Table: staging.stg_us_fl_company

-- DROP TABLE IF EXISTS staging.stg_us_fl_company;

CREATE TABLE IF NOT EXISTS staging.stg_im_company
(
    company_number character varying(20) COLLATE pg_catalog."default",
    name character varying(255) COLLATE pg_catalog."default",
    jurisdiction_code character varying(10) COLLATE pg_catalog."default",
    current_status character varying(50) COLLATE pg_catalog."default",
    company_type character varying(100) COLLATE pg_catalog."default",
    incorporation_date jsonb,
    dissolution_date jsonb,
    registry_url character varying(255) COLLATE pg_catalog."default",
    officers jsonb,
    registered_address character varying(500) COLLATE pg_catalog."default",
    mailing_address jsonb,
    headquarters_address jsonb,
    company_identifiers jsonb,
    all_attributes jsonb,
    previous_names jsonb,
    source_name character varying(50) COLLATE pg_catalog."default",
    fetched_by character varying(50) COLLATE pg_catalog."default",
    fetched_at timestamp without time zone,
    parsed_by character varying(50) COLLATE pg_catalog."default",
    parsed_at timestamp without time zone,
    cdm_mapping_rules character varying(50) COLLATE pg_catalog."default",
	cdm_mapped_by character varying(50) COLLATE pg_catalog."default",
    cdm_mapped_at timestamp without time zone,
    extracted_at timestamp without time zone DEFAULT now()
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS staging.stg_us_fl_company
    OWNER to postgres;