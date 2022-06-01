raw_taxi_data_valid = {
    "total_amount_exists": "total_amount is not null",
    "trip_distance_is_positive": "trip_distance > 0",
}

raw_taxi_data_warn = {
    "passenger_count_less_than_five": "passenger_count < 5",
}

enriched_taxi_data_valid = {
    **raw_taxi_data_valid,
    "short_ride_exists": "short_ride is not null",
}

enriched_taxi_data_warn = raw_taxi_data_warn
