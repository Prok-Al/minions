from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

RESTAURANTS_URL = (
    "https://api.prod.digital.uni.rest/api/mobile/bff/api/v1"
    "/store/get_restaurants?showClosed=true"
)
MENU_URL_TEMPLATE = "https://rostics.ru/api/menu/getmenu/{store_id}/ru/clickcollect"
MENU_HEADERS = {"Origin": "https://rostics.ru"}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

TARGET_PRODUCT_IDS: tuple[str, ...] = ("1602107", "1602276", "1602286")

OUTPUT_FILE = Path("data.json")
REQUEST_TIMEOUT = 30
MAX_WORKERS = 8
MAX_RETRIES = 5
BACKOFF_FACTOR = 1.0
PROXY_ENV_VAR = "ROSTICS_PROXY"


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    proxy = os.environ.get(PROXY_ENV_VAR, "").strip()
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
        logger.info("Запросы идут через прокси")
    else:
        logger.info("Прокси не задан, прямое соединение")

    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_restaurants(session: requests.Session) -> list[dict[str, Any]]:
    response = session.get(RESTAURANTS_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json().get("searchResults", [])


def fetch_menu(session: requests.Session, store_id: str) -> dict[str, Any] | None:
    url = MENU_URL_TEMPLATE.format(store_id=store_id)
    response = session.get(url, headers=MENU_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json().get("value")


CATALOG_EXCLUDED_FIELDS = frozenset({"price", "stopList"})
STORE_EXCLUDED_FIELDS = frozenset(
    {"features", "deliveryTypes", "numberOfTables", "services"}
)


def find_available_products(menu: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    products = menu.get("products", {})
    available: list[tuple[str, dict[str, Any]]] = []
    for product_id in TARGET_PRODUCT_IDS:
        product = products.get(product_id)
        if product and not product.get("stopList"):
            available.append((product_id, product))
    return available


def trim_store_public(store_public: dict[str, Any]) -> None:
    opening_hours = store_public.get("openingHours")
    if isinstance(opening_hours, dict):
        store_public["openingHours"] = {"regular": opening_hours.get("regular")}
    for field in STORE_EXCLUDED_FIELDS:
        store_public.pop(field, None)


def build_catalog_entry(product: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in product.items()
        if key not in CATALOG_EXCLUDED_FIELDS
    }


def process_restaurant(
    session: requests.Session, restaurant: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]] | None:
    store_id = restaurant["storePublic"]["storeId"]
    try:
        menu = fetch_menu(session, store_id)
    except requests.RequestException as exc:
        logger.warning("Не удалось получить меню %s: %s", store_id, exc)
        return None

    if menu is None:
        logger.warning("Меню недоступно для ресторана %s", store_id)
        return None

    available_products = find_available_products(menu)
    if not available_products:
        return None

    trim_store_public(restaurant["storePublic"])
    restaurant["products"] = [
        {"id": product_id, "price": product.get("price")}
        for product_id, product in available_products
    ]
    catalog = {
        product_id: build_catalog_entry(product)
        for product_id, product in available_products
    }
    logger.info("Найден целевой ресторан: %s", store_id)
    return restaurant, catalog


def collect_target_restaurants(session: requests.Session) -> dict[str, Any]:
    restaurants = fetch_restaurants(session)
    logger.info("Получено ресторанов: %d", len(restaurants))

    target_restaurants: list[dict[str, Any]] = []
    products_catalog: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_restaurant, session, restaurant)
            for restaurant in restaurants
        ]
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                continue
            restaurant, catalog = result
            target_restaurants.append(restaurant)
            for product_id, entry in catalog.items():
                products_catalog.setdefault(product_id, entry)

    logger.info("Итого целевых ресторанов: %d", len(target_restaurants))
    return {"products": products_catalog, "restaurants": target_restaurants}


def save_results(data: dict[str, Any], path: Path = OUTPUT_FILE) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=4),
        encoding="utf-8",
    )
    logger.info("Результат сохранён в %s", path)


def main() -> None:
    with build_session() as session:
        data = collect_target_restaurants(session)
    save_results(data)


if __name__ == "__main__":
    main()
