from click import command, argument, option

from . import run


@command()
@option("--semesters", help="Number of semesters to crawl", default=2)
@argument("database")
def main(semesters: int, database: str):
    run(semesters, database)


if __name__ == "__main__":
    main()
