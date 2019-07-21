import asyncio
import json

from click import argument, command, option

from . import start


@command()
@option("--semesters", help="Number of semesters to crawl", default=2)
@argument("output")
def main(semesters: int, output: str):
    data = asyncio.run(start(semesters))

    with open(output, "w") as f:
        json.dump(data, f)


if __name__ == "__main__":
    main()
