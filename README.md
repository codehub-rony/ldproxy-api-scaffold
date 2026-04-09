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

#### Overriding or adding custom configurations

The configuration generator supports custom overrides via the custom_config parameter.

This allows you to modify both:

Service level settings (provider configuration)
API building blocks (e.g. HTML, QUERYABLES, TILES)

All overrides are deep-merged, meaning nested values are safely updated without replacing entire objects.

```
custom_config = {
    'service': {
        'api': {
            'HTML':  {"homeUrl": "https://dummy.test/cool"},
            'QUERYABLES':  {"enabled": False},
        }
    },
    'provider': {
        'entityStorageVersion': 3,
        'queryGeneration': {'computeNumberMatched': False}
    }
}


config_generator = APIConfig(
    service_id=service_id,
    schema_name="public",
    db_conn_str=db_con_str,
    api_blocks=["QUERYABLES", "CRS", "FILTER", "TILES", "STYLES", "PROJECTIONS", "HTML"],
    custom_config=custom_config

)
```

The above code will generate the following config:

```
//services/<service_id>.yml

- buildingBlock: HTML
  enabled: true
  homeUrl: https://dummy.com
```

```
//providers/<service_id>.yml

id: <service_id>
entityStorageVersion: 3
createdAt: 1775732757
lastModified: 1775732757
providerType: FEATURE
providerSubType: SQL
...
typeValidation: NONE
queryGeneration:
  computeNumberMatched: false
```
