#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import pendulum
import requests
import datetime
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

class IncrementalHarvestForecastStream(HarvestForecastStream, ABC):

    date_param_template = "%Y%m%d"
    cursor_field = "updated_at"

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, **kwargs)
        params = {**params, **stream_slice} if stream_slice else params
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_benchmark = latest_record[self.cursor_field]
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: max(latest_benchmark, current_stream_state[self.cursor_field])}
        return {self.cursor_field: latest_benchmark}

class Projects(IncrementalHarvestForecastStream):
    """
    Path: "https://api.forecastapp.com/projects"
    Docs: "This API is still in BETA and where is no official docs (https://help.getharvest.com/forecast/faqs/faq-list/api/)
    """


class Clients(HarvestForecastStream):
    """
    Path: "https://api.forecastapp.com/cients"
    Docs: "This API is still in BETA and where is no official docs (https://help.getharvest.com/forecast/faqs/faq-list/api/)
    """


class Roles(HarvestForecastStream):
    """
    Path: "https://api.forecastapp.com/roles"
    Docs: "This API is still in BETA and where is no official docs (https://help.getharvest.com/forecast/faqs/faq-list/api/)
    """


class Milestones(IncrementalHarvestForecastStream):
    """
    Path: "https://api.forecastapp.com/milestones"
    Docs: "This API is still in BETA and where is no official docs (https://help.getharvest.com/forecast/faqs/faq-list/api/)
    """


class People(IncrementalHarvestForecastStream):
    """
    Path: "https://api.forecastapp.com/people"
    Docs: "This API is still in BETA and where is no official docs (https://help.getharvest.com/forecast/faqs/faq-list/api/)
    """

class HarvestForecastDateRangeStream(HarvestForecastStream):

    date_param_template = "%Y%m%d"
    cursor_field = "updated_at"

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, **kwargs)
        params = {**params, **stream_slice} if stream_slice else params
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_benchmark = latest_record[self.cursor_field]
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: max(latest_benchmark, current_stream_state[self.cursor_field])}
        return {self.cursor_field: latest_benchmark}

    def stream_slices(self, sync_mode, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[MutableMapping[str, any]]]:
        """
        Override default stream_slices CDK method to provide date_slices as page chunks for data fetch.
        """
        slices = []

        # Create first slice from 180days ago to today
        start_date = pendulum.now().date().subtract(days=360)
        end_date = pendulum.now().date().subtract(days=180)
        slices.append({"start_date": start_date.strftime(self.date_param_template), "end_date": end_date.strftime(self.date_param_template)})

        # Create first slice from 180days ago to today
        start_date = pendulum.now().date().subtract(days=180)
        end_date = pendulum.now().date()
        slices.append({"start_date": start_date.strftime(self.date_param_template), "end_date": end_date.strftime(self.date_param_template)})

        # Create second slice for today to 180days out
        start_date = pendulum.now().date()
        end_date = pendulum.now().date().add(days=180)
        slices.append({"start_date": start_date.strftime(self.date_param_template), "end_date": end_date.strftime(self.date_param_template)})

        # Create second slice for today to 180days out
        start_date = pendulum.now().date().add(days=180)
        end_date = pendulum.now().date().add(days=360)
        slices.append({"start_date": start_date.strftime(self.date_param_template), "end_date": end_date.strftime(self.date_param_template)})

        return slices

class Assignments(HarvestForecastDateRangeStream):
    """
    Path: "https://api.forecastapp.com/assignments"
    Docs: "This API is still in BETA and where is no official docs (https://help.getharvest.com/forecast/faqs/faq-list/api/)
    """
