import json
from typing import Any, Dict, Iterable, List, Optional, Union

import requests
import xmltodict
from memoization import cached
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from pendulum import parse

from tap_exact.auth import OAuth2Authenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from time import sleep
from singer_sdk.helpers._state import increment_state
import datetime
import re
from lxml import etree
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer.schema import Schema

import singer
from singer import StateMessage

REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"


class ExactStream(RESTStream):

    def __init__(
        self,
        tap: TapBaseClass,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None,
        path: Optional[str] = None,
    ) -> None:
        super().__init__(tap, name=name, schema=schema, path=path)
        if self.config.get("warehouse_uuid"):
            self._tap.warehouse_uuid = self.config.get("warehouse_uuid")

    dont_use_current_division = False
    default_rep_key_field = "Modified"

    @property
    def url_base(self) -> str:
        url = self.config.get("auth_url", self.config.get("uri")) or "https://start.exactonline.nl/api/oauth2/token"

        if "oauth2" in url:
            url = re.findall("(.*)/oauth2", url)[0]
        elif "api" not in url:
            url = f"{url}/api"

        if self.dont_use_current_division:
            return url.replace("/api", "")

        current_division = self.config.get("current_division")
        url = f"{url}/v1/{current_division}"
        return url

    records_jsonpath = "$.feed.entry[*]"
    ignore_parent_stream = False


    @property
    def authenticator(self) -> OAuth2Authenticator:
        oauth_url = self.config.get("auth_url", self.config.get("uri")) or "https://start.exactonline.nl/api/oauth2/token"
        if "/api/oauth2" not in oauth_url:
            oauth_url = f"{oauth_url}/api/oauth2/token"
        if not oauth_url.endswith("/token"):
            oauth_url += "/token"
        return OAuth2Authenticator(self, self.config, auth_endpoint=oauth_url)

    @property
    def http_headers(self) -> dict:
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    @property
    def default_warehouse_uuid(self) -> str:
        if not self._tap.warehouse_uuid:
            self.get_warehouse_uuid
        return self._tap.warehouse_uuid

    @property
    def get_warehouse_uuid(self) -> str:
        if self.config.get("default_warehouse_id"):
            default_warehouse_id = self.config.get("default_warehouse_id")
            url = f"{self.url_base}/inventory/Warehouses"
            params = {"$filter": f"Code eq '{default_warehouse_id}'"}
            self.logger.info(f"PARAMS - {params}")
            headers = self.authenticator.auth_headers

            response = requests.request("GET", url=url, params=params, headers=headers)
            self.logger.info(f"RESPONSE TEXT : {response.text}")
            self.validate_response(response)
            res_json = self.xml_to_dict(response)

            warehouse_res = res_json["feed"].get("entry")
            if warehouse_res:
                warehouse_uuid = warehouse_res["content"]["m:properties"][
                    "d:ID"
                ]["#text"]
            else:
                raise Exception(f"No warehouses found with Code {default_warehouse_id}")

            self._tap._config["warehouse_uuid"] = warehouse_uuid
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self._tap._config, outfile, indent=4)
            self._tap.warehouse_uuid = warehouse_uuid
        elif not self.config.get("use_stock_multiple_warehouses", False):
            raise Exception("There is no default_warehouse_code")

    @property
    def sync_endpoint(self):
        if self.config.get("sync_endpoints") != None:
            return self.config.get("sync_endpoints")
        return False

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        res_json = self.xml_to_dict(response)
        if "link" in res_json.get("feed", {}).keys():
            link_dict = {}
            links = res_json["feed"]["link"]
            if type(links) == list:
                for record in res_json["feed"]["link"]:
                    link_dict[record["@rel"]] = record["@href"]
                if "next" in link_dict.keys():
                    next_link = link_dict["next"]
                    next_page_token = next_link.split("&")[-1]
                    next_page_token = next_page_token.split("=")[-1]
                    return next_page_token
        else:
            return None

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {}
        if self.select:
            params["$select"] = self.select
        start_date = self.get_starting_time(context)
        start_date = start_date + datetime.timedelta(seconds=1)
        start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        filter = None
        date_filter = None
        if (
            self.replication_key
            and self.replication_key != "Timestamp"
            and start_date
        ):
            date_filter = f"{self.default_rep_key_field} gt datetime'{start_date}'"
            params["$filter"] = date_filter
        if self.config.get("sync_endpoints") != None:
            if hasattr(self, "filter"):
                filter = self.filter
            if filter and date_filter:
                params["$filter"] = f"{filter} and {date_filter}"
            elif filter or date_filter:
                params["$filter"] = filter or date_filter
        if hasattr(self, "expand"):
            params["$expand"] = self.expand
        if next_page_token:
            params["$skiptoken"] = next_page_token
        return params

    def xml_to_dict(self, response):
        try:
            #clean invalid xml characters
            my_parser = etree.XMLParser(recover=True)
            xml = etree.fromstring(response.content, parser=my_parser)
            cleaned_xml_string = etree.tostring(xml)
            #parse xml to dict
            data = json.loads(json.dumps(xmltodict.parse(cleaned_xml_string)))
        except:
            data = json.loads(json.dumps(xmltodict.parse(response.content.decode("utf-8-sig").encode("utf-8"))))
        return data

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        return extract_jsonpath(self.records_jsonpath, input=self.xml_to_dict(response))

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        content = row["content"]["m:properties"]
        new_content = {}
        for key in content:
            if type(content[key]) == type(""):
                new_content[key[2:]] = content[key]
            elif "Edm.Boolean" == (content.get(key) or {}).get("@m:type"):
                if content[key].get("#text") == "true":
                    new_content[key[2:]] = True
                elif content[key].get("#text") == "false":
                    new_content[key[2:]] = False
                else:
                    new_content[key[2:]] = None
            else:
                new_content[key[2:]] = (content.get(key) or {}).get("#text", None)
        row = new_content
        return row

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        use_bill_of_materials_versions = (
            self.config.get("use_bill_of_materials_versions", True)
        )
        use_sales_orders = (
            self.config.get("use_sales_orders")
            if self.config.get("use_sales_orders") != None
            else True
        )
        use_production_orders = (
            self.config.get("use_production_orders")
            if self.config.get("use_production_orders") != None
            else True
        )
        use_sales_invoices = (
            self.config.get("use_sales_invoices")
            if self.config.get("use_sales_invoices") != None
            else False
        )
        use_stock_multiple_warehouses = (
            self.config.get("use_stock_multiple_warehouses") or False
        )
        use_bill_of_materials = (
            self.config.get("use_bill_of_materials", True)
        )
        use_assembly_orders = (
            self.config.get("use_assembly_orders")
            if self.config.get("use_assembly_orders") != None
            else True
        )
        use_exchange_rates = (
            self.config.get("use_exchange_rates")
            if self.config.get("use_exchange_rates") != None
            else True
        )
        use_assembly_bill_of_material_header = (
            self.config.get("use_assembly_bill_of_material_header", True)
        )
        use_assembly_bill_of_material_materials = (
            self.config.get("use_assembly_bill_of_material_materials", True)
        )
        use_price_lists = (
            self.config.get("use_price_lists", True)
        )

        if (
            (self.name == "sales_order" and not use_sales_orders)
            or (self.name == "manufacturing_shop_orders" and not use_production_orders)
            or (self.name == "sales_invoices" and not use_sales_invoices)
            or (self.name == "warehouses" and use_stock_multiple_warehouses)
            or (self.name == "exchange_rates" and not use_exchange_rates)
            or (self.name == "sales_prices_linked_accounts" and not use_price_lists)
            or (self.name == "sales_prices_lists" and not use_price_lists)
            or (self.name == "sales_prices_list_periods" and not use_price_lists)
            or (self.name == "sales_price_list_volume_discounts" and not use_price_lists)
            or (
                self.name == "logistics_stock_positions"
                and not use_stock_multiple_warehouses
            )
            or (
                self.name == "bill_of_material_download"
                and not use_bill_of_materials
            )
            or (self.name == "assembly_orders" and not use_assembly_orders)
            or (
                self.name == "bill_of_materials_versions"
                and not use_bill_of_materials_versions
            )
            or (
                self.name == "assembly_bill_of_material_header"
                and not use_assembly_bill_of_material_header
            )
            or (
                self.name == "assembly_bill_of_material_materials"
                and not use_assembly_bill_of_material_materials
            )
            or (self.name == "bill_of_materials" and not use_bill_of_materials_versions)
        ):
            pass
        else:
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    continue
                yield transformed_record

    def validate_response(self, response: requests.Response) -> None:
        sleep(1.01)
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(f"{msg} with response {response.text}")
        elif response.status_code == 408:
            self.logger.info(f"Retrying after timeout")
            raise RetriableAPIError("Retrying after error")
        elif response.status_code == 429:
            rate_limit_reset = int(response.headers.get("X-RateLimit-Reset")) / 1000
            # if limit per day is exceeded raise error
            if response.headers.get("X-RateLimit-Remaining") == 0:
                raise FatalAPIError(f"Rate limit exceeded per day, please wait until {datetime.datetime.fromtimestamp(int(rate_limit_reset))} to try again")
            msg = self.response_error_message(response)
            raise RetriableAPIError(f"{msg} with response {response.text}")
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            raise FatalAPIError(f"{msg} with response {response.text}")

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name]["partitions"] = []

        singer.write_message(StateMessage(value=tap_state))