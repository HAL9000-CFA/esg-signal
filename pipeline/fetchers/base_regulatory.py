import os
from abc import ABC, abstractmethod

import pandas as pd


class BaseRegulatoryFetcher(ABC):
    """
    Base class for mandatory regulatory data fetchers (GHGRP, ECHO, NRC, EA, EU ETS).
    Unlike BaseFetcher, these return pandas DataFrames and write to disk.
    US fetchers are invoked when EDGAR succeeds; UK/EU fetchers when Companies House succeeds.
    """

    @abstractmethod
    def fetch(self, company_name: str, year: int = None) -> pd.DataFrame:
        """
        Download and filter data for the given company.
        Returns an empty DataFrame (not raises) if the company is not found.
        """
        pass

    def save(self, df: pd.DataFrame, raw_path: str, processed_path: str) -> None:
        """Write raw and processed CSVs, creating parent directories as needed."""
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        os.makedirs(os.path.dirname(processed_path), exist_ok=True)
        df.to_csv(raw_path, index=False)
        df.to_csv(processed_path, index=False)
