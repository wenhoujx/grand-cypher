from grandcypher.constants import MODELS, NAME, SOURCE


def source_name(schema, entity_type): 
    return next((s[SOURCE] for s in  schema[MODELS] if s[NAME] == entity_type), None)
