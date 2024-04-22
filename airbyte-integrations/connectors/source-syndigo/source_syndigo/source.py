#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
# from urllib.parse import parse_qs, urlparse

import requests

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from .client import SyndigoAPIClient
from abc import ABC

logger = logging.getLogger("airbyte")

class SyndigoStream(HttpStream, ABC):

    MAX_TAKE_COUNT = 10000

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self.syndigo_client = SyndigoAPIClient(config.get("username"), config.get("secret_key"))

    @property
    def url_base(self) -> str:
        """
        :return: URL base for the  API endpoint
        """
        return self.syndigo_client.url_base

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Override to return any headers.
        """
        return {
            'Content-Type': 'application/json',
            'Authorization': self.syndigo_client.auth.get("AuthHeader")
        }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        As per Syndigo docs they have 'skip' & 'take' query params for pagination,
        "Appendix B: Report Generation Guide" at https://pages.syndigo.com/rs/539-CDH-942/images/Report_Builder_API_v2.3_May_23.pdf

        However, 'skip' doesn't work...

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        # try:
        #     parsed_url = urlparse(response.request.url)
        #     query_params = parse_qs(parsed_url.query, keep_blank_values=True, strict_parsing=True)
        #     query_params = {key: values[0] for key, values in query_params.items()}
        #     skip = int(query_params.get('skip', '0'))
        #     take = int(query_params.get('take', self.MAX_TAKE_COUNT))
        #     total_count = response.json().get("TotalHitCount")
        # 
        #     if (skip + take) < total_count:
        #         skip += take
        #         return {
        #             'skip': skip,
        #             'take': take
        #         }
        # except Exception as e:
        #     logger.error("Failed to get next_page_token")
        #     logger.error(e)
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        Usually contains common params e.g. pagination size etc.

        Initial request: {'skip': 0, 'take': total_records_available}
        """
        params = {
            'skip': 0,
            "take": self.MAX_TAKE_COUNT
        }
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        try:
            response.raise_for_status()
            res = response.json()
            for id in res.get("Results"):
                yield {"id": id}
        except Exception as e:
            logger.error("Failed to parse_response")
            logger.error(e)
        yield {}


class SearchProduct(SyndigoStream):

    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(config, **kwargs)
        self.config = config

    @property
    def http_method(self) -> str:
        return "POST"

    def request_body_json(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        """
        Override when creating POST/PUT/PATCH requests to populate the body of the request with a JSON payload.

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        return self.config.get("search_product_request_body")

    def path(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "api/search/productsearch/scroll"

class SourceSyndigo(AbstractSource):
    def _raise_if_invalid(self, config):
        required_fields = ["username", "secret_key"]
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            raise Exception(f"Required fields missing: [{', '.join(missing_fields)}]")


    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            self._raise_if_invalid(config)
            client = SyndigoAPIClient(config.get("username"), config.get("secret_key"))
            schema = client.auth.get("Scheme")
            logger.info(f"Authenticated with Schema: '{schema}'")
            return True, None
        except Exception as error:
            return False, error

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [SearchProduct(config=config)]
