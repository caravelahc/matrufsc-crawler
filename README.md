# matrufsc-crawler

Crawler to extract information from UFSC's CAGR service.

# Intro
This project aims to generate a JSON with all relevant course data from UFSC's ['Cadastro de Turmas'](https://cagr.sistemas.ufsc.br/modules/comunidade/cadastroTurmas/) page.

# Setup
This project uses `poetry` as the dependency manager. Install dependencies and run with:
```
poetry install
poetry run matrufsc-crawler ./db.json
```

If you have `pip>=19` installed, it already supports PEP517 build backends, so you can just pip install it.

# Usage as library
```python
import matrufsc_crawler as crawler

data = crawler.run()
with open("./db.json", "w") as f:
    json.dump(data, f)
```
