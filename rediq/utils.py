import os
import sys
import re

from pytz import timezone
from itertools import chain
from datetime import datetime
from importlib import import_module
from typing import Any, Optional


def import_from_name(name: str) -> Any:
    try:
        module_name, attr_name, *ext = name.split(':')
        assert len(ext) == 0
    except Exception as e:
        raise ValueError('"app_name" must be in format "module_name:attribute_name"') from e
    
    cwd = os.getcwd()
    sys.path.append(cwd)

    try:
        module = import_module(name=module_name, package=None)
    finally:
        try:
            sys.path.remove(cwd)
        except:
            pass

    return getattr(module, attr_name)


'''
POSIX Cron:
 ┌───────────── minute (0–59)
 │ ┌───────────── hour (0–23)
 │ │ ┌───────────── day of the month (1–31)
 │ │ │ ┌───────────── month (1–12)
 │ │ │ │ ┌───────────── day of the week (0–6)
 │ │ │ │ │                                   
 │ │ │ │ │
 │ │ │ │ │
 * * * * *
'''
class CronParser:

    _regexp = re.compile(r"(?P<start>(\d+)|\*)(-(?P<end>\d+))?(/(?P<step>\d+))?")
    _month_replacements = { v: str(k + 1) for k, v in enumerate(['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']) }
    _weekday_replacements = { v: str(k) for k, v in enumerate(['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']) }

    @classmethod
    def _parse_item(
        cls,
        expression: str, 
        min_value: int, 
        max_value: int, 
        replacements: dict = None,
    ):
        
        expression = expression.upper()
        if replacements:
            for k, v in replacements.items():
                expression = expression.replace(k, v)

        matches = cls._regexp.match(expression)
        if not matches:
            raise ValueError(f'invalid expression: "{expression}"')

        start = matches.group("start")
        end = matches.group("end") or start
        step = matches.group("step") or 1

        if start == "*":
            start = min_value
            end = max_value
        
        values = range(int(start), int(end) + 1, int(step))

        for value in values:
            if not min_value <= value <= max_value:
                raise ValueError(f"{expression} produces items out of {min_value}-{max_value}")
        
        return values
    
    @classmethod
    def _parse_group(
        cls,
        expression: str, 
        min_value: int, 
        max_value: int, 
        replacements: dict = None,
    ):
 
        groups = [ cls._parse_item(item, min_value, max_value, replacements) for item in expression.split(',') ]
        return set(chain(*groups))
    
    @classmethod
    def _parse(cls, expression: str):
        try:
            minutes, hours, monthdays, months, weekdays, *items = expression.split(' ')
            assert len(items) == 0
        except Exception as e:
            raise ValueError(f"invalid number of items in expression: {expression}") from e
        return [
            cls._parse_group(minutes, 0, 59),
            cls._parse_group(hours, 0, 23),
            cls._parse_group(monthdays, 1, 31),
            cls._parse_group(months, 1, 12, cls._month_replacements),
            cls._parse_group(weekdays, 0, 6, cls._weekday_replacements),
        ]

    def __init__(self, expression: str):
        self._data = self._parse(expression)
    
    def is_now(self, tzname: Optional[str] = None):
        date = datetime.now(timezone(tzname) if tzname else None)
        minute, hour, monthday, month, weekday = date.minute, date.hour, date.day, date.month, date.weekday()
        return all((
            minute in self._data[0],
            hour in self._data[1],
            monthday in self._data[2],
            month in self._data[3],
            weekday in self._data[4],
        ))