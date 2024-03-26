import json
import logging
from typing import List

import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]) -> bool:

        # Оновлення часових міток оброблених даних агента
        for item in processed_agent_data_batch:
            item.agent_data.timestamp = item.agent_data.timestamp.isoformat()
            response = requests.post(f"{self.api_base_url}/processed_agent_data/", json=item.model_dump())

            if( response.status_code != requests.codes.ok ):
                return False

        return True

