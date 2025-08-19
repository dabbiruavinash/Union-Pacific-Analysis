class TrainConstants:
    TRAIN_SCHEMA = [
        "train_id", "train_number", "origin", "destination", 
        "departure_time", "arrival_time", "status", "current_speed",
        "max_speed", "weight", "length", "car_count"
    ]
    
class ShipmentConstants:
    SHIPMENT_SCHEMA = [
        "shipment_id", "train_id", "commodity_type", "weight", 
        "volume", "origin", "destination", "status", "priority"
    ]