import re
from datetime import timedelta

_DURATION = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)


def parse_duration(value: str) -> timedelta:
    match = _DURATION.fullmatch(value)
    if match is None or not any(match.groupdict().values()):
        raise ValueError("duration must be a positive ISO 8601 day-time duration")
    duration = timedelta(**{name: int(raw or 0) for name, raw in match.groupdict().items()})
    if duration <= timedelta(0):
        raise ValueError("duration must be greater than zero")
    return duration
