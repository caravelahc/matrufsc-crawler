import json

from click import command, argument, Path, option

from . import run


@command()
@argument('database', type=Path(exists=False))
@option('--semesters', help='Number of semesters to crawl', default=2)
def main(database: Path, semesters: int):
    data = run(semesters)
    with open(database, 'w') as f:
        json.dump(data, f)


if __name__ == '__main__':
    main()
