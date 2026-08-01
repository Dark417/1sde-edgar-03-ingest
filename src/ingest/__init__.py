"""EDGAR -> landing zone batch ingest (repo 3 of the edgar lakehouse).

Fetches from SEC EDGAR and writes raw records to the landing zone. Owns no
schema (those come from ``edgar_lakehouse_contracts``), no Spark, and no
transformation: payloads pass through verbatim.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
