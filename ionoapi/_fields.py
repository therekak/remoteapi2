from __future__ import annotations

from typing import Any, Callable

from pydantic import BaseModel, GetCoreSchemaHandler
from pydantic_core import core_schema, CoreSchema
from pydantic.json_schema import JsonSchemaValue

from pytimeparse2 import parse as timeparse
from datetime import timedelta


class Hex(str):

    @classmethod
    def __get_pydantic_json_schema__(
            cls, core_schema: core_schema.JsonSchema, handler: Callable
        ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema.update(
            pattern='^[0-9A-Fa-f]{1,9}$',
            examples=['13ff', 'c56fds'],
        )
        return json_schema

    @classmethod
    def validate(cls, __input_value: Any, _):
        if not isinstance(__input_value, str):
            raise TypeError('string required')

        v = __input_value
        try:
            vv = hex(int(v, 16))
        except Exception as e:
            raise ValueError('invalid hex format')

        return cls(vv)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type[Any], handler: Callable[[Any], core_schema.CoreSchema]
    ) -> core_schema.CoreSchema:
        return core_schema.with_info_plain_validator_function(cls.validate)

    def __repr__(self):
        return f'Hex({super().__repr__()})'


class Bool(str):

    @classmethod
    def __get_pydantic_json_schema__(
            cls, core_schema: core_schema.JsonSchema, handler: Callable
    ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema.update(
            pattern='^[0,1]{1}$',
            examples=['0', '1'],
        )
        return json_schema

    @classmethod
    def validate(cls, __input_value: Any, _):
        if not isinstance(__input_value, str):
            raise TypeError('string required')

        v = __input_value
        try:
            vv = bool(int(v))
        except Exception as e:
            raise ValueError('invalid bool format')

        return cls(vv)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type[Any], handler: Callable[[Any], core_schema.CoreSchema]
    ) -> core_schema.CoreSchema:
        return core_schema.with_info_plain_validator_function(cls.validate)

    def __repr__(self):
        return f'Bool({super().__repr__()})'


class Delta(str):

    @classmethod
    def __get_pydantic_json_schema__(
            cls, core_schema: core_schema.JsonSchema, handler: Callable
        ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema.update(
            title='Human Readable Time-delta parser',
            pattern='^[0-9A-Fa-f]{1,9}$',
            examples=['1w3d2h32m', '5hr34m56s', '5 hrs, 34 mins, 56 secs', '5.6 weeks'],
            type='string($duration)',
        )
        return json_schema

    @classmethod
    def validate(cls, __input_value: str, _) -> Delta:
        if not isinstance(__input_value, str):
            raise TypeError('string required')

        v = __input_value
        try:
            vv = timeparse(v)
        except Exception as e:
            raise ValueError(f'invalid Delta format: {e}')

        return cls(vv)

    @classmethod
    def __get_pydantic_core_schema__(
        #cls, source: type[Any], handler: Callable[[Any], core_schema.CoreSchema]
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        #return core_schema.json_or_python_schema(
        #    json_schema=core_schema.str_schema(),
        #    python_schema=core_schema.union_schema([
        #        core_schema.is_instance_schema(str),
        #        core_schema.chain_schema([
        #            core_schema.str_schema(),
        #            core_schema.no_info_plain_validator_function(cls.validate),
        #        ])
        #    ]),
        #    serialization=core_schema.plain_serializer_function_ser_schema(
        #        lambda x: str(x)
        #    ),
        #)

        return core_schema.with_info_after_validator_function(cls.validate, handler(str))

        #return core_schema.with_info_plain_validator_function(cls.validate)

    def __repr__(self):
        return f'Delta({super().__repr__()})'