from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime

# Data model for raw bronze layer ingestion
class ChicagoTripRaw(BaseModel):
    trip_id: str = Field(alias="trip_id")
    taxi_id: str = Field(alias="taxi_id")
    trip_start_timestamp: str = Field(alias="trip_start_timestamp")
    trip_end_timestamp: Optional[str] = Field(alias="trip_end_timestamp")
    trip_seconds: Optional[float] = Field(alias="trip_seconds")
    trip_miles: Optional[float] = Field(alias="trip_miles")
    pickup_community_area: Optional[float] = Field(alias="pickup_community_area")
    dropoff_community_area: Optional[float] = Field(alias="dropoff_community_area")
    fare: Optional[float] = Field(alias="fare")
    tips: Optional[float] = Field(alias="tips")
    tolls: Optional[float] = Field(alias="tolls")
    extras: Optional[float] = Field(alias="extras")
    trip_total: Optional[float] = Field(alias="trip_total")
    payment_type: Optional[str] = Field(alias="payment_type")
    company: Optional[str] = Field(alias="company")
    pickup_latitude: Optional[float] = Field(alias="pickup_centroid_latitude")
    pickup_longitude: Optional[float] = Field(alias="pickup_centroid_longitude")
    dropoff_latitude: Optional[float] = Field(alias="dropoff_centroid_latitude")
    dropoff_longitude: Optional[float] = Field(alias="dropoff_centroid_longitude")

    class Config:
        populate_by_name = True

# Data model for silver layer transformation
class ChicagoTripClean(BaseModel):
    trip_id: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_min: float
    distance_miles: float
    pickup_area: int
    dropoff_area: Optional[int]
    total_cost: float
    payment_type: str
    company: str

    # Enforce business logic validation
    @field_validator('total_cost')
    @classmethod
    def cost_must_be_positive(cls, v: float) -> float:
        if v < 0:
            raise ValueError('Cost cannot be negative')
        return v