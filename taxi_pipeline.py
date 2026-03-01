"""dlt pipeline to ingest NYC Taxi data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


@dlt.source
def nyc_taxi_source():
    """Define dlt resources from the NYC Taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "rides",
                "endpoint": {
                    "path": "/",
                    "paginator": PageNumberPaginator(
                        base_page=1,
                        page_param="page",
                        total_path=None,          # API doesn't return total pages
                        stop_after_empty_page=True,  # Stop when empty page returned
                    ),
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_source())
    print(load_info)  # noqa: T201