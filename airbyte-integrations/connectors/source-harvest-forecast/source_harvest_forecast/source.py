#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

# import pendulum
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from source_harvest_forecast.streams import Projects

from .auth import HarvestOauth2Authenticator, HarvestTokenAuthenticator


class SourceHarvestForecast(AbstractSource):
    @staticmethod
    def get_authenticator(config):
        credentials = config.get("credentials", {})
        if credentials and "client_id" in credentials:
            return HarvestOauth2Authenticator(
                token_refresh_endpoint="https://id.getharvest.com/api/v2/oauth2/token",
                client_id=credentials.get("client_id"),
                client_secret=credentials.get("client_secret"),
                refresh_token=credentials.get("refresh_token"),
                account_id=config["account_id"],
            )

        api_token = credentials.get("api_token", config.get("api_token"))
        if not api_token:
            raise Exception("Config validation error: 'api_token' is a required property")
        return HarvestTokenAuthenticator(token=api_token, account_id=config["account_id"])

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            auth = self.get_authenticator(config)
            # replication_start_date = pendulum.parse(config["replication_start_date"])
            Projects(authenticator=auth).read_records(sync_mode=SyncMode.full_refresh)
            # next(users_gen)
            return True, None
        except Exception as error:
            return False, f"Unable to connect to Harvest Forecast API with the provided credentials - {repr(error)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = self.get_authenticator(config)
        # replication_start_date = pendulum.parse(config["replication_start_date"])
        # from_date = replication_start_date.date()

        streams = [
            Projects(authenticator=auth),
        ]

        return streams
