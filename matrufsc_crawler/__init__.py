import logging
import re

from enum import Enum
from math import ceil
from queue import Queue
from threading import Thread

import requests

from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.FileHandler("crawler.log", mode="w")

formatter = logging.Formatter("%(asctime)s - %(message)s", datefmt=r"%Y/%m/%d %T")

handler.setFormatter(formatter)
logger.addHandler(handler)


CAGR_URL = "https://cagr.sistemas.ufsc.br/modules/comunidade/cadastroTurmas/"


class Campus(Enum):
    # EAD = 0
    FLO = 1
    JOI = 2
    CBS = 3
    ARA = 4
    BLN = 5


def _available_semesters(logger):
    logger.debug("Requesting main page to get semesters")
    html = requests.get(CAGR_URL).text
    logger.debug("Received main page")

    soup = BeautifulSoup(html, "html.parser")
    select = soup.find("select", id="formBusca:selectSemestre")

    logger.debug("Returning available semesters")

    return [option["value"] for option in select.find_all("option")]


class StopMessage:
    pass


RESULTS_COUNT_REGEX = re.compile(r"(\d+)</span> resultados foram encontrados")


def _crawl(campus: Campus, semester: str, send_queue: Queue):
    label = f"crawler_{campus.name}_{semester}"

    logger.info(f"[{label}] Starting crawler")
    session = requests.Session()

    logger.debug(f"[{label}] Requesting main page to get cookies")
    session.post(CAGR_URL)
    logger.debug(f"[{label}] Received main page")

    form_data = {
        "formBusca": "formBusca",
        "javax.faces.ViewState": "j_id1",
        "formBusca:selectSemestre": semester,
        "formBusca:selectCampus": campus.value,
    }

    # we don't use the first page for arcane reasons
    form_data["formBusca:dataScroller1"] = 2
    second_page = session.post(CAGR_URL, form_data)

    results_count_matches = RESULTS_COUNT_REGEX.findall(second_page.text)
    if not results_count_matches:
        message = f"[{label}] Could not find result count."
        logger.error(message)
        send_queue.put(StopMessage)
        raise Exception(message)

    results_count = int(results_count_matches[0])
    pages_count = ceil(results_count / 50)

    previous = ""
    for page_index in range(pages_count):
        page = page_index + 1

        form_data["formBusca:dataScroller1"] = page
        logger.info(f"[{label}] Requesting page {page}")

        response = session.post(CAGR_URL, form_data)
        logger.debug(f"[{label}] Received page {page}")

        if response.url != CAGR_URL:
            message = f"[{label}] Received {response.url} instead of page {page}"
            logger.error(message)
            send_queue.put(StopMessage)
            raise Exception(message)

        if response.text == previous:
            logger.warning(f"[{label}] Received repeated page")
            break

        logger.info(f"[{label}] Pushing contents of page {page} to queue")
        send_queue.put(response.text)
        previous = response.text

    logger.debug(f"[{label}] Pushing StopMessage to queue")
    send_queue.put(StopMessage)
    logger.info(f"[{label}] Stopping crawler")


def _parse(campus: Campus, semester: str, receive_queue: Queue, output: dict):
    label = f"parser_{campus.name}_{semester}"

    logger.info(f"[{label}] Starting parser")

    while True:
        logger.debug(f"[{label}] Waiting for message in queue")
        message = receive_queue.get()
        logger.debug(f"[{label}] Popping message from queue")

        if message is StopMessage:
            logger.debug(f"[{label}] Got StopMessage from queue")
            receive_queue.task_done()
            break

        soup = BeautifulSoup(message, "html.parser")
        table = soup.find("tbody", id="formBusca:dataTable:tb")
        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            fields = (x.get_text("\n", strip=True) for x in cells)

            (
                _,
                _,
                _,
                course_id,
                class_id,
                course_name,
                class_hours,
                capacity,
                enrolled,
                special,
                _,
                waiting,
                times,
                professors,
            ) = fields

            course_name, *class_labels = course_name.splitlines()
            class_hours = int(class_hours) if class_hours else None
            capacity = int(capacity) if capacity else None
            enrolled = int(enrolled) if enrolled else None
            special = int(special) if special else None
            waiting = int(waiting) if waiting else None
            times = times.splitlines()
            professors = professors.splitlines()

            course = output.setdefault(course_id, {})
            course["course_name"] = course_name
            course["class_hours"] = class_hours

            classes = course.setdefault("classes", {})
            classes[class_id] = {
                "class_labels": [l.strip("[]") for l in class_labels],
                "capacity": capacity,
                "enrolled": enrolled,
                "special": special,
                "waiting": waiting,
                "times": times,
                "professors": professors,
            }

        logger.debug(f"[{label}] Got {len(rows)} entries in page contents")
        receive_queue.task_done()

    logger.info(f"[{label}] Stopping parser")


def _start(campus: Campus, semester: str, outputs: dict):
    label = f"starter_{campus.name}_{semester}"

    logger.info(f"[{label}] Creating queue, crawler and parser")

    queue = Queue()
    args = (campus, semester, queue)

    crawler = Thread(target=_crawl, args=args)

    output = outputs.setdefault(semester, {}).setdefault(campus.name, {})

    parser = Thread(target=_parse, args=(*args, output))

    crawler.start()
    parser.start()

    crawler.join()
    logger.debug(f"[{label}] Crawler stopped")

    queue.join()
    logger.debug(f"[{label}] Queue done")

    parser.join()
    logger.debug(f"[{label}] Parser stopped")


def run(n_semesters: int = 2):
    outputs = {}
    semesters = _available_semesters(logger)[:n_semesters]

    threads = [
        Thread(target=_start, args=(campus, semester, outputs))
        for campus in Campus
        for semester in semesters
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    return outputs
