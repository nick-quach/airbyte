#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream

# from urllib.parse import parse_qsl, urlparse


class HarvestForecastStream(HttpStream, ABC):
    url_base = "https://api.forecastapp.com/"
    primary_key = "id"

    @property
    def data_field(self) -> str:
        """
        :return: Default field name to get data from response
        """
        return self.name

    def backoff_time(self, response: requests.Response):
        if "Retry-After" in response.headers:
            return int(response.headers["Retry-After"])
        else:
            self.logger.info("Retry-after header not found. Using default backoff value")
            return super().backoff_time(response)

    def path(self, **kwargs) -> str:
        return self.name

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        stream_data = response.json()

        # depending on stream type we may get either:
        # * nested records iterable in response object;
        # * not nested records iterable;
        # * single object to yield.
        if self.data_field:
            stream_data = response.json().get(self.data_field, [])

        if isinstance(stream_data, list):
            yield from stream_data
        else:
            yield stream_data


class Projects(HarvestForecastStream):
    """
    Docs: https://help.getharvest.com/api-v2/clients-api/clients/contacts/
    """
