import asyncio
import json
import re

from enum import Enum
from math import ceil
from typing import List, Dict, Union, Optional, AsyncIterator

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from yarl import URL


CAGR = URL("https://cagr.sistemas.ufsc.br/modules/comunidade/cadastroTurmas/")


class Campus(Enum):
    # EAD = 0
    FLO = 1
    JOI = 2
    CBS = 3
    ARA = 4
    BLN = 5


async def _available_semesters() -> List[str]:
    async with ClientSession() as session:
        response = await session.get(CAGR)
        contents = await response.text()

    document = BeautifulSoup(contents, "html.parser")
    select = document.find("select", id="formBusca:selectSemestre")

    return [option["value"] for option in select.find_all("option")]


RESULTS_COUNT_REGEX = re.compile(r"(\d+)</span> resultados foram encontrados")


async def _pages_to_crawl(
    session: ClientSession, form_data: Dict[str, Union[str, int]]
) -> int:
    # we don't use the first page for arcane reasons
    form_data["formBusca:dataScroller1"] = 2
    second_page_response = await session.post(CAGR, data=form_data)
    second_page_contents = await second_page_response.text()

    matches = RESULTS_COUNT_REGEX.findall(second_page_contents)
    if not matches:
        return 0

    results_count = int(matches[0])
    return ceil(results_count / 50)


async def _fetch(campus: Campus, semester: str) -> AsyncIterator[str]:
    async with ClientSession() as session:
        await session.post(CAGR)

        form_data = {
            "formBusca": "formBusca",
            "javax.faces.ViewState": "j_id1",
            "formBusca:selectSemestre": semester,
            "formBusca:selectCampus": campus.value,
        }

        pages_count = await _pages_to_crawl(session, form_data)

        previous = ""
        for page_index in range(1, pages_count + 1):
            form_data["formBusca:dataScroller1"] = page_index

            response = await session.post(CAGR, data=form_data)

            if response.url != CAGR:
                raise Exception(
                    f"Received unexpected URL '{response.url}'"
                    f"instead of page {page_index} for ({campus}, {semester})"
                )

            contents = await response.text()

            if contents == previous:
                break

            yield contents
            previous = contents


TIME_PLACE_REGEX = re.compile(r"(\d)\.(\d{4})-(\d) / (.+)")
TIME_SLOTS = [
    "0730",
    "0820",
    "0910",
    "1010",
    "1100",
    "1330",
    "1420",
    "1510",
    "1620",
    "1710",
    "1830",
    "1920",
    "2020",
    "2110",
]


def _parse_time_and_place(s: str):
    match = TIME_PLACE_REGEX.match(s.strip())
    if match is None:
        return None

    weekday, time, duration, room = match.groups()

    start = TIME_SLOTS.index(time)
    end = start + int(duration)

    return {
        "weekday": int(weekday) - 2,  # turns monday into 0
        "slots": TIME_SLOTS[start:end],
        "room": room,
    }


def _parse(contents: str, output: dict):
    soup = BeautifulSoup(contents, "html.parser")
    table = soup.find("tbody", id="formBusca:dataTable:tb")

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        fields = [x.get_text("\n", strip=True) for x in cells]

        course_id = fields[3]
        class_id = fields[4]

        course_name, *class_labels = fields[5].splitlines()
        class_labels = [l.strip("[]") for l in class_labels]

        def parse_int(x: str) -> Optional[int]:
            return int(x) if x else None

        class_hours = parse_int(fields[6])
        capacity = parse_int(fields[7])
        enrolled = parse_int(fields[8])
        special = parse_int(fields[9])
        waiting = parse_int(fields[11])

        times_and_places = fields[12].splitlines()
        times_and_places = [_parse_time_and_place(s) for s in times_and_places]
        professors = fields[13].splitlines()

        course = output.setdefault(course_id, {})
        course["name"] = course_name
        course["class_hours"] = class_hours

        classes = course.setdefault("classes", {})
        classes[class_id] = {
            "labels": class_labels,
            "capacity": capacity,
            "enrolled": enrolled,
            "special": special,
            "waiting": waiting,
            "times_and_places": times_and_places,
            "professors": professors,
        }


async def _crawl(campus: Campus, semester: str, output: dict):
    output = output.setdefault(semester, {})
    output = output.setdefault(campus.name, {})

    async for page in _fetch(campus, semester):
        _parse(page, output)


async def _start(n_semesters: int, output_path: str):
    semesters = (await _available_semesters())[:n_semesters]

    output: Dict = {}
    tasks = (_crawl(c, s, output) for c in Campus for s in semesters)
    await asyncio.gather(*tasks)

    with open(output_path, "w") as f:
        json.dump(output, f)


def run(n_semesters: int, output_path: str):
    asyncio.run(_start(n_semesters, output_path))
