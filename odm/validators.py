"""
BaseValidator module.
It serves as an base to all other validators childs.
"""

from datetime import datetime
import jsonschema
from jsonschema import Draft4Validator
from bson.objectid import ObjectId

_types = {
    'object_id': ObjectId
}


class ValidationError(Exception):
    def __init__(self, *args, errors: list=None, messages: list=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.messages = messages
        self.errors = errors


class JsonSchemaValidator():
    TYPES = _types
    SCHEMA = dict()
    VALIDATOR = None

    def __init__(self, schema: dict, additional_types: dict=None, **kwargs):
        """
        Arguments:
            schema {dict} -- valida jsonschema
        Keyword Arguments:
            additional_types {dict} -- additional types to be checked against (default: {None})
        """

        self.SCHEMA = schema
        if additional_types:
            self.TYPES = dict(self.TYPES, **additional_types)
        self.VALIDATOR = Draft4Validator(self.SCHEMA, types=self.TYPES)
        self.VALIDATOR.VALIDATORS['method'] = self.__method

    def __method(self, validator, fn, instance, schema):
        try:
            fn(instance, validator=validator)
        except Exception as e:
            yield ValidationError("%r failed for %r: %r" % (instance, fn.__name__, e))

    def validate(self, instance):
        messages = list()
        errors = list()
        for e in self.VALIDATOR.iter_errors(instance):
            errors.append(e)
            messages.append(e.schema.get('description', e.message))
        if len(errors):
            msg = '\n'.join(messages)
            raise ValidationError(msg, messages=messages, errors=errors)
        return True

def validate_once(instance, schema, additional_types: dict=None, **kwargs):
    validator = JsonSchemaValidator(schema, additional_types=additional_types, **kwargs)
    validator.validate(instance)
    return True


class BaseValidator:
    """
    BaseValidator class.

    :method validate(ruleSet, methods): Validation main function.
    :method validate_datetime(field, rule): Validates if a field is a datatime.
    :method validate_integer(field, rule): Validates if a field is an integer.
    :method validate_float(field, rule): Validates if a field is a float.
    :method validate_str(field, rule): Validates if a field is a string.
    :method validate_objectid(field, rule): Validates if a field is an objectid.
    :method validate_object(field, rule): Validates if a field is an object.
    :method validate_array(field, rule): Validates if a field is an array
    """
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    REQUIRED = "required"
    OPTIONAL = "optional"
    MISSING = "missing"

    def __init__(self, params):

        self.params = params
        self.errors = []
        self.rules = {

        }

    def validate(self, rule_set, methods=[]):
        """
        Validation main function.

        :param rule_set: Sets of rules to be validated.
        :param methods: Methods to be called.
        :return: Return is there was any error.
        """
        valid = True
        if self.rules.get(rule_set) is None:
            raise Exception("O Conjunto de regras " +
                            rule_set + " não esta definido.")

        for k, rule in self.rules[rule_set].items():
            if rule.get("presence") is None:
                raise Exception(
                    "A propriedade presence é obrigatória na regra " + k)

            if rule.get("format") is None:
                raise Exception(
                    "A propriedade format é obrigatória na regra " + k)

            if rule["presence"] == self.REQUIRED and self.params.get(k) is None:
                self.errors.append("Atributo '" + k +
                                   "' é requerido e não está presente")

            elif rule["presence"] == self.MISSING and self.params.get(k) is not None:
                self.errors.append("Atributo '" + k +
                                   "' não pode estar preenchido")

            elif self.params.get(k) is not None:
                if "datetime" == rule["format"]:
                    if not self.validate_datetime(self.params[k], rule):
                        self.errors.append(
                            "A data '" + k + "' esta em um formato inválido: " + self.params[k])

                if "integer" == rule["format"]:
                    if not self.validate_integer(self.params[k], rule):
                        self.errors.append(
                            "O inteiro '" + k + "' esta em um formato inválido: " + self.params[k])

                if "float" == rule["format"]:
                    if not self.validate_float(self.params[k], rule):
                        self.errors.append(
                            "O decimal '" + k + "' esta em um formato inválido: " + self.params[k])

                if "string" == rule["format"]:
                    if not self.validate_str(self.params[k], rule):
                        self.errors.append(
                            "A string '" + k + "' esta em um formato inválido")

                if "array" == rule["format"]:
                    if not self.validate_array(self.params[k], rule):
                        self.errors.append(
                            "O array '" + k + "' esta em um formato inválido")

                if "objectid" == rule["format"]:
                    if not self.validate_objectid(self.params[k], rule):
                        self.errors.append(
                            "O ObjectId '" + k + "' esta em um formato inválido: " + self.params[k])

                if "object" == rule["format"]:
                    if not self.validate_object(self.params[k], rule):
                        self.errors.append(
                            "O Object '" + k + "' esta em um formato inválido")

                if "boolean" == rule["format"]:
                    if not self.validate_boolean(self.params[k], rule):
                        self.errors.append(
                            "O Boolean '" + k + "' esta em um formato inválido")

        for method in methods:
            m = getattr(self, method)
            if m is not None:
                m()

        return len(self.errors) == 0

    def validate_datetime(self, field: str, rule: dict = {}):
        """
        Validates if a field is a datatime.

        :param field: Field to be validated.
        :param rule: Rule to be used for validation.
        :return: Boolean value depending on the validation results.
        """
        try:
            a = datetime.strptime(field[:19], "%Y-%m-%dT%H:%M:%S")
            return True
        except Exception as e:
            return False

    def validate_integer(self, field: str, rule: dict = {}):
        """
        Validates if a field is an integer.

        :param field: Field to be validated.
        :param rule: Rule to be used for validation.
        :return: Boolean value depending on the validation results.
        """
        try:
            a = int(field)
            return True
        except Exception as e:
            return False

    def validate_float(self, field: str, rule: dict = {}):
        """
        Validates if a field is a float.

        :param field: Field to be validated.
        :param rule: Rule to be used for validation.
        :return: Boolean value depending on the validation results.
        """
        try:
            a = float(field)
            return True
        except Exception as e:
            return False

    def validate_str(self, field: str, rule: dict = {}):
        """
        Validates if a field is a string.

        :param field: Field to be validated.
        :param rule: Rule to be used for validation.
        :return: Boolean value depending on the validation results.
        """
        try:
            valid = True
            a = str(field)
            if rule.get("min"):
                valid = valid and len(a) >= rule["min"]
            if rule.get("max"):
                valid = valid and len(a) <= rule["max"]

            return valid
        except Exception as e:
            return False

    def validate_objectid(self, field: str, rule: dict = {}):
        """
        Validates if a field is an objectid.

        :param field: Field to be validated.
        :param rule: Rule to be used for validation.
        :return: Boolean value depending on the validation results.
        """
        try:
            valid = True
            a = ObjectId(field)
            return True
        except Exception as e:
            return False

    def validate_object(self, field: str, rule: dict = {}):
        """
        Validates if a field is an object.

        :param field: Field to be validated.
        :param rule: Rule to be used for validation.
        :return: Boolean value depending on the validation results.
        """
        try:
            valid = True
            a = dict(field)
            return True
        except Exception as e:
            return False

    def validate_array(self, field: str, rule: dict = {}):
        """
        Validates if a field is an array.

        :param field: Field to be validated.
        :param rule: Rule to be used for validation.
        :return: Boolean value depending on the validation results.
        """
        try:
            valid = True
            a = list(field)
            if rule.get("min"):
                valid = valid and len(a) >= rule["min"]
            if rule.get("max"):
                valid = valid and len(a) <= rule["max"]
            return valid
        except Exception as e:
            return False

    def validate_boolean(self, field: str, rule: dict = {}):
        """
        Validates if a field is an boolean.

        :param field: Field to be validated.
        :param rule: Rule to be used for validation.
        :return: Boolean value depending on the validation results.
        """
        try:
            return type(field) == bool
        except Exception as e:
            return False
