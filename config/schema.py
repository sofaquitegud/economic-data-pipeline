# Import libraries
from datetime import date
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, validator


class BaseEconomicRecord(BaseModel):
    """Base model with common fields"""

    date: date
    source: str = "datagovmy"

    class Config:
        extra = "forbid"  # Reject unexpected fields


class GDPRecord(BaseEconomicRecord):
    """GDP quarterly data validation"""

    series: Literal["abs", "growth"]
    value: float = Field(description="GDP value in RM millions or percentage")

    @validator("value")
    def validate_gdp_value(cls, v: float, values: dict[str, Any]) -> float:
        if values.get("series") == "abs" and v < 0:
            raise ValueError("Absolute GDP cannot be negative")
        return v


class CPIRecord(BaseEconomicRecord):
    """Consumer Price Index validation"""

    category: str
    value: float = Field(gt=0, description="CPI index value")
    base_year: int = Field(ge=2000, le=2030)


class LabourRecord(BaseEconomicRecord):
    """Labour market data validation"""

    metric: Literal["employed", "unemployed", "labour_force", "unemployment_rate"]
    value: float = Field(ge=0)
    unit: Literal["thousands", "percentage"]


class ExchangeRateRecord(BaseEconomicRecord):
    """Exchange rate validation"""

    currency_pair: str = Field(pattern=r"^[A-Z]{3}/[A-Z]{3}$")
    rate: float = Field(gt=0)


class PopulationRecord(BaseEconomicRecord):
    """Population data validation"""

    age_group: Optional[str] = None
    sex: Optional[Literal["male", "female", "total"]] = "total"
    state: Optional[str] = None
    count: int = Field(ge=0)
