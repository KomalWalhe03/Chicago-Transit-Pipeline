import pytest
from datetime import datetime
from src.models import ChicagoTripClean
from pydantic import ValidationError

# Test Case 1: Schema Validation Success
def test_valid_trip_model():
    """
    Verifies that a correctly formatted record instantiates the 
    ChicagoTripClean model without raising validation errors.
    """
    trip = ChicagoTripClean(
        trip_id="12345",
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration_min=15.5,
        distance_miles=3.2,
        pickup_area=8,
        dropoff_area=32,
        total_cost=25.50, 
        payment_type="Credit Card",
        company="Flash Cab"
    )
    # Assertions to ensure data integrity is preserved
    assert trip.trip_id == "12345"
    assert trip.total_cost == 25.50

# Test Case 2: Business Logic Enforcement
def test_negative_cost_fails():
    """
    Verifies that the custom validator correctly raises a ValidationError 
    when a negative monetary value is provided for total_cost.
    """
    with pytest.raises(ValidationError):
        ChicagoTripClean(
            trip_id="12345",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_min=15.5,
            distance_miles=3.2,
            pickup_area=8,
            dropoff_area=32,
            total_cost=-10.00, # Invalid: Negative cost violates business logic
            payment_type="Cash",
            company="Flash Cab"
        )

# Test Case 3: Type Safety & Coercion Failure
def test_invalid_type_conversion():
    """
    Verifies that Pydantic enforces strict type checking by raising a 
    ValidationError when an incompatible type (e.g., string) is passed 
    to a numeric field.
    """
    with pytest.raises(ValidationError):
        ChicagoTripClean(
            trip_id="12345",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_min="fifteen minutes", # Invalid: String provided for float field
            distance_miles=3.2,
            pickup_area=8,
            dropoff_area=32,
            total_cost=25.50,
            payment_type="Cash",
            company="Flash Cab"
        )