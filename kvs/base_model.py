from pydantic import BaseModel


class Model(BaseModel):
    """our base model"""
    class Config:
        allow_population_by_field_name = True
        smart_union = True

class ImmutableModel(Model):
    """
    our base model, but (faux-)immutable.
    should be used for any models that might get passed around / stored in multiple places, to avoid
    any accidental side effects from mutating them in-place
    """
    class Config(Model.Config):
        allow_mutation = False
