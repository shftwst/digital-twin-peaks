import argparse
import hashlib
import os
from argparse import Namespace

from alembic import command
from alembic.config import Config

MIGRATION_SERVICES = ("control", "relay", "identity", "crm")


def migration_lock_key(service: str) -> int:
    digest = hashlib.sha256(f"enterprise-twins:migration:{service}".encode()).digest()
    return int.from_bytes(digest[:8], signed=False) & ((1 << 63) - 1)


def upgrade(service: str, database_url: str) -> None:
    if service not in MIGRATION_SERVICES:
        raise ValueError(f"unknown migration service: {service}")
    configuration = Config("alembic.ini")
    configuration.attributes["migration_service"] = service
    configuration.attributes["database_url"] = database_url
    configuration.cmd_opts = Namespace(x=[f"service={service}"])
    command.upgrade(configuration, "head")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("service", choices=MIGRATION_SERVICES)
    args = parser.parse_args()
    upgrade(args.service, os.environ["TWINS_DATABASE_URL"])


if __name__ == "__main__":
    main()
