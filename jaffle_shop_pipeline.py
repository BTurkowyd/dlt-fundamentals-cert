"""
Jaffle Shop API dlt Pipeline

This pipeline extracts data from the Jaffle Shop API and loads it to DuckDB.
Optimized with parallelization and chunking for better performance.
"""

import os
import dlt
import requests
from typing import Iterator, List, Dict, Any
from dlt.common.typing import TDataItems


API_BASE = "https://jaffle-shop.scalevector.ai/api/v1"


def paginate(endpoint: str) -> Iterator[List[Dict[str, Any]]]:
    """Paginate through API endpoint, yielding pages (chunks) for efficiency."""
    page = 1
    while True:
        url = f"{API_BASE}/{endpoint}?page={page}&page_size=100"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        yield data

        if "next" not in response.links:
            break
        page += 1


@dlt.resource(
    table_name="customers",
    write_disposition="replace",
    primary_key="id",
    parallelized=True,
)
def customers() -> TDataItems:
    """Extract customers from Jaffle Shop API."""
    yield from paginate("customers")


@dlt.resource(
    table_name="orders",
    write_disposition="replace",
    primary_key="id",
    parallelized=True,
)
def orders() -> TDataItems:
    """Extract orders from Jaffle Shop API."""
    yield from paginate("orders")


@dlt.resource(
    table_name="products",
    write_disposition="replace",
    primary_key="sku",
    parallelized=True,
)
def products() -> TDataItems:
    """Extract products from Jaffle Shop API."""
    yield from paginate("products")


@dlt.source
def jaffle_shop_source():
    """Jaffle Shop API source with all resources."""
    return customers, orders, products


def run_pipeline():
    """Run the Jaffle Shop pipeline."""
    # Configure workers for optimal performance
    os.environ.setdefault("EXTRACT__WORKERS", "3")
    os.environ.setdefault("NORMALIZE__WORKERS", str(os.cpu_count() or 2))
    os.environ.setdefault("LOAD__WORKERS", "5")

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop",
        destination="duckdb",
        dataset_name="jaffle_shop_data",
    )

    load_info = pipeline.run(jaffle_shop_source())
    print(load_info)

    return load_info


if __name__ == "__main__":
    run_pipeline()
