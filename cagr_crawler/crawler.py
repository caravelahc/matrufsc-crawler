import logging

from enum import Enum
from itertools import count
from queue import Queue
from threading import Thread

import requests
from bs4 import BeautifulSoup


CAGR_URL = 'https://cagr.sistemas.ufsc.br/modules/comunidade/cadastroTurmas/'


class Campus(Enum):
    # EAD = 0
    FLO = 1
    JOI = 2
    CBS = 3
    ARA = 4
    BLN = 5


def available_semesters(logger):
    logger.debug('Requesting main page to get semesters')
    html = requests.get(CAGR_URL).text
    logger.debug('Received main page')

    soup = BeautifulSoup(html, 'html.parser')
    select = soup.find('select', id='formBusca:selectSemestre')

    logger.debug('Returning available semesters')

    return [
        option['value']
        for option in select.find_all('option')
    ]


class StopMessage:
    pass


def crawl(campus: Campus, semester: str, queue: Queue, logger):
    logger.info(f'Starting crawler')

    logger.debug(f'Requesting main page to get cookies')
    cookies = requests.get(CAGR_URL).cookies
    logger.debug(f'Received main page')

    form_data = {
        'AJAXREQUEST': '_viewRoot',
        'formBusca': 'formBusca',
        'javax.faces.ViewState': 'j_id1',

        'formBusca:selectSemestre': semester,
        'formBusca:selectCampus': campus.value,
    }

    previous = ''
    for page in count(1):
        form_data['formBusca:dataScroller1'] = page
        logger.debug(f'Requesting page {page}')
        response = requests.post(CAGR_URL, form_data, cookies=cookies)
        logger.debug(f'Received page {page}')

        if response.url != CAGR_URL:
            message = (
                f'Received {response.url} instead of page {page}'
            )
            logger.error(message)
            raise Exception(message)

        if response.text == previous:
            logger.debug(f'Received repeated page')
            break

        logger.debug(f'Pushing contents of page {page} to queue')
        queue.put(response.text)
        previous = response.text

    logger.debug(f'Pushing StopMessage to queue')
    queue.put(StopMessage)
    logger.info(f'Stopping crawler')


def parse(campus: Campus, semester: str, queue: Queue, logger):
    logger.info(f'Starting parser')

    while True:
        logger.debug(f'Waiting for message in queue')
        message = queue.get()
        logger.debug(f'Popping message from queue')

        if message is StopMessage:
            logger.debug(f'Got StopMessage from queue')
            queue.task_done()
            break

        soup = BeautifulSoup(message, 'html.parser')
        table = soup.find('tbody', id='formBusca:dataTable:tb')
        rows = table.find_all('tr')

        for row in rows:
            cells = row.find_all('td')
            fields = (x.get_text('\n', strip=True) for x in cells)

            (_, _, _, course_id, class_id, course_name, class_time,
             capacity, enrolled, special, _, waiting,
             times, professor) = fields

            # TODO: export these in some way

        logger.debug(f'Got {len(rows)} entries in page contents')
        queue.task_done()

    logger.info(f'Stopping parser')


def run(campus: Campus, semester: str, logger):
    logger.info(f'Creating queue, crawler and parser')

    queue = Queue()
    args = (campus, semester, queue, logger)

    crawler = Thread(
        target=crawl,
        args=args,
        name=f'crawler_{campus.name}_{semester}',
    )

    parser = Thread(
        target=parse,
        args=args,
        name=f'parser_{campus.name}_{semester}',
    )

    crawler.start()
    parser.start()

    crawler.join()
    logger.debug(f'Crawler stopped')

    queue.join()
    logger.debug(f'Queue done')

    parser.join()
    logger.debug(f'Parser stopped')


def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    handler = logging.FileHandler('crawler.log', mode='w')

    formatter = logging.Formatter(
        '%(asctime)s - %(threadName)s - %(message)s',
        datefmt=r'%Y/%m/%d %T'
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    threads = [
        Thread(
            target=run,
            args=(campus, semester, logger),
            name=f'runner_{campus.name}_{semester}'
        )
        for campus in Campus
        for semester in available_semesters(logger)[:2]
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
