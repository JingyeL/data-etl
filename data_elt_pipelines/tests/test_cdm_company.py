from shared.cdm_company import CdmCompany, validate_field_exists


def test_values():
    assert CdmCompany.COMPANY_NUMBER.value == "company_number"
    assert CdmCompany.NAME.value == "name"
    assert CdmCompany.JURISDICTION_CODE.value == "jurisdiction_code"
    assert CdmCompany.CURRENT_STATUS.value == "current_status"
    assert CdmCompany.COMPANY_TYPE.value == "company_type"
    assert CdmCompany.INCORPORATION_DATE.value == "incorporation_date"
    assert CdmCompany.DISSOLUTION_DATE.value == "dissolution_date"
    assert CdmCompany.REGISTRY_URL.value == "registry_url"
    assert CdmCompany.OFFICERS.value == "officers"
    assert CdmCompany.REGISTERED_ADDRESS.value == "registered_address"
    assert CdmCompany.MAILING_ADDRESS.value == "mailing_address"
    assert CdmCompany.HEADQUARTERS_ADDRESS.value == "headquarters_address"
    assert CdmCompany.COMPANY_IDENTIFIERS.value == "company_identifiers"
    assert CdmCompany.ALL_ATTRIBUTES.value == "all_attributes"
    assert CdmCompany.PREVIOUS_NAMES.value == "previous_names"


def test_enum_order():
    assert CdmCompany.COMPANY_NUMBER.order == 1
    assert CdmCompany.NAME.order == 2
    assert CdmCompany.JURISDICTION_CODE.order == 3
    assert CdmCompany.CURRENT_STATUS.order == 4
    assert CdmCompany.COMPANY_TYPE.order == 5
    assert CdmCompany.INCORPORATION_DATE.order == 6
    assert CdmCompany.DISSOLUTION_DATE.order == 7
    assert CdmCompany.REGISTRY_URL.order == 8
    assert CdmCompany.OFFICERS.order == 9
    assert CdmCompany.REGISTERED_ADDRESS.order == 10
    assert CdmCompany.MAILING_ADDRESS.order == 11
    assert CdmCompany.HEADQUARTERS_ADDRESS.order == 12
    assert CdmCompany.COMPANY_IDENTIFIERS.order == 13
    assert CdmCompany.ALL_ATTRIBUTES.order == 14
    assert CdmCompany.PREVIOUS_NAMES.order == 15


def test_enum_comparison():
    assert CdmCompany.COMPANY_NUMBER < CdmCompany.NAME
    assert CdmCompany.NAME > CdmCompany.COMPANY_NUMBER
    assert CdmCompany.JURISDICTION_CODE < CdmCompany.CURRENT_STATUS
    assert CdmCompany.CURRENT_STATUS > CdmCompany.JURISDICTION_CODE
    

def test_enum_values():
    assert CdmCompany.COMPANY_NUMBER.value == "company_number"
    assert CdmCompany.NAME.value == "name"
    assert CdmCompany.JURISDICTION_CODE.value == "jurisdiction_code"
    assert CdmCompany.CURRENT_STATUS.value == "current_status"
    assert CdmCompany.COMPANY_TYPE.value == "company_type"
    assert CdmCompany.INCORPORATION_DATE.value == "incorporation_date"
    assert CdmCompany.DISSOLUTION_DATE.value == "dissolution_date"
    assert CdmCompany.REGISTRY_URL.value == "registry_url"
    assert CdmCompany.OFFICERS.value == "officers"
    assert CdmCompany.REGISTERED_ADDRESS.value == "registered_address"
    assert CdmCompany.MAILING_ADDRESS.value == "mailing_address"
    assert CdmCompany.HEADQUARTERS_ADDRESS.value == "headquarters_address"
    assert CdmCompany.COMPANY_IDENTIFIERS.value == "company_identifiers"
    assert CdmCompany.ALL_ATTRIBUTES.value == "all_attributes"
    assert CdmCompany.PREVIOUS_NAMES.value == "previous_names"


def test_validate():
    assert validate_field_exists("company_number") is True
    assert validate_field_exists("name") is True
    assert validate_field_exists("jurisdiction_code") is True
    assert validate_field_exists("current_status") is True
    assert validate_field_exists("company_type") is True
    assert validate_field_exists("incorporation_date") is True
    assert validate_field_exists("dissolution_date") is True
    assert validate_field_exists("registry_url") is True
    assert validate_field_exists("officers") is True
    assert validate_field_exists("registered_address") is True
    assert validate_field_exists("mailing_address") is True
    assert validate_field_exists("headquarters_address") is True
    assert validate_field_exists("company_identifiers") is True
    assert validate_field_exists("all_attributes") is True
    assert validate_field_exists("previous_names") is True
    assert validate_field_exists("non_existent_field") is False

