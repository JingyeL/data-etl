from enum import Enum

class OrderedEnum(Enum):
    def __new__(cls, value, order):
        obj = object.__new__(cls)
        obj._value_ = value
        obj._order_ = order
        return obj

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self._order_ < other._order_
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self._order_ > other._order_
        return NotImplemented

    @property
    def order(self):
        return self._order_


class CdmCompany(OrderedEnum):
    COMPANY_NUMBER = ("company_number", 1)
    NAME = ("name", 2)
    JURISDICTION_CODE = ("jurisdiction_code", 3)
    CURRENT_STATUS = ("current_status", 4)
    COMPANY_TYPE = ("company_type", 5)
    INCORPORATION_DATE = ("incorporation_date", 6)
    DISSOLUTION_DATE = ("dissolution_date", 7)
    REGISTRY_URL = ("registry_url", 8)
    OFFICERS = ("officers", 9)
    REGISTERED_ADDRESS = ("registered_address", 10)
    MAILING_ADDRESS = ("mailing_address", 11)
    HEADQUARTERS_ADDRESS = ("headquarters_address", 12)
    COMPANY_IDENTIFIERS = ("company_identifiers", 13)
    ALL_ATTRIBUTES = ("all_attributes", 14)
    PREVIOUS_NAMES = ("previous_names", 15)
    PERIODICITY = ("periodicity", 16)
    FETCHED_BY = ("fetched_by", 16)
    FETCHED_AT = ("fetched_at", 17)
    PARSED_BY = ("parsed_by", 18)
    PARSED_AT = ("parsed_at", 20)
    MAPPING_RULES = ("cdm_mapping_rules", 21)
    CDM_MAPPED_BY = ("cdm_mapped_by", 22)
    CDM_MAPPED_AT = ("cdm_mapped_at", 23)
    SOURCE_NAME = ("source_name", 24)
    HASH = ("hash", 25)
    SOURCE_TIMESTAMP = ("source_timestamp", 26)


def validate_field_exists(field: str):
    return field in [e.value for e in CdmCompany]
