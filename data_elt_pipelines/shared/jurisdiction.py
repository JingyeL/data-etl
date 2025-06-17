from enum import Enum
from pydantic import Field, BaseModel, model_validator, ValidationError

class Jurisdiction(str, Enum):
    US_FL = 'us_fl'
    IM = 'im'


class JurisdictionModel(BaseModel):
    jurisdiction: Jurisdiction

    @model_validator(mode='before')
    def check_jurisdiction(cls, values):
        jurisdiction = values.get('jurisdiction')
        if jurisdiction not in Jurisdiction:
            raise ValidationError(f'Invalid jurisdiction: {jurisdiction}')
        return values