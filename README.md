# ldproxy config generator

A simple python script for generating basic API configuration files for [ldproxy](https://github.com/interactive-instruments/ldproxy) based on a database connection. The generated config files will be saved in a `export` folder. The folder structure inside the `export` folder can be copied to ldproxy data folder under `store/entities/`

Currently the following API building blocks are supported

- `QUERYABLES`
- `TILES`
- `CRS`
- `STYLES`
- `HTML`

See [ldproxy docs](https://docs.ldproxy.net/services/building-blocks/) for more information

### Example code

```
from ldproxy_api_scaffold import APIConfig

gen = APIConfig(service_id=<service_id>, schema_name=<schema_name>,
                             db_conn_str='postgresql://<db_user>:<db_pass>@localhost:5432.<db_name>',
                             db_host_template_str='<host_template>', run_in_docker=True)


export_dir = 'store/entities'

gen.generate(export_dir=export_dir)
```

#### Generating config files with custom params

You can customize the generated configuration files by providing additional parameters that override or extend the default settings of specific API block

```
params = {
    "HTML": {"homeUrl": "https://dummy.com"},

}


config_generator = APIConfig(
    service_id=service_id,
    schema_name="public",
    db_conn_str=db_con_str,
    api_blocks=["QUERYABLES", "CRS", "FILTER", "TILES", "STYLES", "PROJECTIONS", "HTML"],
    api_block_params=params

)
```

The above code will generate the following config:

```
//services/<service_id>.yml

- buildingBlock: HTML
  enabled: true
  homeUrl: https://dummy.com
```
