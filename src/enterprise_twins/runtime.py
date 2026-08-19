import argparse
import os

import uvicorn

from enterprise_twins.migration_runner import MIGRATION_SERVICES, upgrade


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("service", choices=MIGRATION_SERVICES)
    parser.add_argument("application")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    upgrade(args.service, os.environ["TWINS_DATABASE_URL"])
    uvicorn.run(
        args.application,
        host="0.0.0.0",  # noqa: S104
        port=args.port,
        factory=True,
    )


if __name__ == "__main__":
    main()
