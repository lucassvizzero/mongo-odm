import asyncio
import logging

import pytest

from odm import BaseModel


def test_split():
    tests = [
        (('a,b', ','), dict(), ['a', 'b']),
        (('a, b', ','), dict(), ['a', 'b']),
        (('a , b', ','), dict(), ['a', 'b']),
        (('a , ,b', ','), dict(), ['a', 'b']),
        (('a,b ', ','), dict(), ['a', 'b']),
        ((' a,b ', ','), dict(), ['a', 'b']),
        ((' a,b ', '.'), dict(), ['a,b']),
        ((' a.b ', '.'), dict(), ['a','b']),
        (('a b', ' '), dict(), ['a','b']),
        (('a,b c', ' '), dict(), ['a,b','c'])
    ]

    model = BaseModel(None)
    
    for args, kwargs, output in tests:
        assert output == model._split(*args, **kwargs)


if __name__ == '__main__':
    pytest.main([__file__])
