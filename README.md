# ldproxy config generator

A simple python script for generating API configuration files for [ldproxy](https://github.com/interactive-instruments/ldproxy). The generated config files will be saved in a `export` folder. The folder structure inside the `export` folder can be copied to ldproxy data folder under `store/entities/`

Currently the following API building blocks are supported

- `QUERYABLES`
- `TILES`
- `CRS`
- `STYLES`

See [ldproxy docs](https://docs.ldproxy.net/services/building-blocks/) for more information

#### Command line interface

A small command line interface is available for going through the steps. Start gui by running `python generator-ui.py` after installing the `requirements.txt`.
