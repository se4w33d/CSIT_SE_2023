from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from dotenv import dotenv_values
from pymongo import MongoClient
from datetime import datetime, date
from dateutil.parser import parse, ParserError
from starlette.requests import Request
from starlette.responses import JSONResponse
import json

config = dotenv_values("env")

app = FastAPI()


@app.on_event("startup")
def startup_db_client():
    app.client = MongoClient(config["DB_URI"])
    app.database = app.client[config["DB_NAME"]]
    print(f"Connected to the {app.database.name} database!")


@app.on_event("shutdown")
def shutdown_db_client():
    app.client.close()


def compute_day_difference(check_in_date: str, check_out_date: str):
    date_out = date.fromisoformat(check_out_date)
    date_in = date.fromisoformat(check_in_date)
    diff = date_out - date_in
    return diff


def validate_date_format(check_in_date: str, check_out_date: str):
    try:
        parse_in = parse(check_in_date, fuzzy=False)
        parse_out = parse(check_out_date, fuzzy=False)
        format_in = date.strftime(parse_in, "%Y-%m-%d")
        format_out = date.strftime(parse_out, "%Y-%m-%d")
        return format_in, format_out
    except ParserError as err:
        raise HTTPException(status_code=400, detail="Wrong input format.")


@app.exception_handler(RequestValidationError)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    # Custom response when a required query parameter is missing
    return JSONResponse(
        status_code=400,
        content={"detail": "Missing required query parameter."},
    )


def validate_destination(cur, destination):
    res = cur.count_documents(destination)
    if res == 0:
        return JSONResponse(
            status_code=200,
            content={"detail": "Don't have such destination."},
        )
    return None


def populate_to_src_dest(res, dict_type):
    for idx, _ in enumerate(res["flights"]):
        flight = dict()
        flight["airline_name"] = res["flights"][idx]["airline"]
        flight["price"] = res["flights"][idx]["price"]
        dict_type["flights"].append(flight)


def retrieve_flights(db_cursor, source, destination, date):
        result = db_cursor.aggregate(
            [
                {
                    "$match": {
                        "srccity": source,
                        "destcity": destination,
                        "date": datetime.fromisoformat(date),
                    }
                },
                {"$project": {"airlinename": 1, "price": 1}},
                {"$sort": {"price": 1}},
            ]
        )
        return result


@app.get("/flight", status_code=200)
def get_flight(departureDate: str, returnDate: str, destination: str):
    app.flights = app.database.flights

    departureDate, returnDate = validate_date_format(departureDate, returnDate)

    cities = ["Singapore", destination]
    cities.sort()

    # departure flights
    departure_result = retrieve_flights(app.flights, "Singapore", destination, departureDate)
    departure_result_list = list(departure_result)
    # return flights
    return_result = retrieve_flights(app.flights, destination, "Singapore", returnDate)
    return_result_list = list(return_result)
    

    if not (departure_result_list or return_result_list):
        responses = []
        responses = json.dumps(responses)
        responses = json.loads(responses)
        return responses

    def find_cheapest(flight):
        if flight["price"] == cheapest["price"]:
            return True
        return False
    
    cheapest = departure_result_list[0]
    cheapest_iter = filter(find_cheapest, departure_result_list)
    cheapest_departure = list(cheapest_iter)

    cheapest = return_result_list[0]
    cheapest_iter = filter(find_cheapest, return_result_list)
    cheapest_return = list(cheapest_iter)

    # generate response
    responses = []
    for depart_f in cheapest_departure:
        for return_f in cheapest_return:
            temp = dict()
            temp["City"] = destination
            temp["Departure Date"] = departureDate
            temp["Departure Airline"] = depart_f["airlinename"]
            temp["Departure Price"] = depart_f["price"]
            temp["Return Date"] = returnDate
            temp["Return Airline"] = return_f["airlinename"]
            temp["Return Price"] = return_f["price"]
            responses.append(temp)

    responses = json.dumps(responses)
    responses = json.loads(responses)
    return responses


@app.get("/hotel", status_code=200)
def get_hotel(checkInDate: str, checkOutDate: str, destination: str):
    app.hotels = app.database.hotels

    checkInDate, checkOutDate = validate_date_format(checkInDate, checkOutDate)

    diff = compute_day_difference(checkInDate, checkOutDate)

    filter = {
        "date": {
            "$gte": datetime.fromisoformat(checkInDate),
            "$lte": datetime.fromisoformat(checkOutDate),
        },
        "city": destination,
    }

    result = app.hotels.aggregate(
        [
            {"$match": filter},
            {
                "$group": {
                    "_id": "$hotelName",
                    "Price2": {"$sum": "$price"},
                    "count": {"$sum": 1},
                }
            },
            {"$match": {"count": diff.days + 1}},
            {"$sort": {"Price2": 1}},
            {"$limit": 5},
            {
                "$addFields": {
                    "City": destination,
                    "Check In Date": checkInDate,
                    "Check Out Date": checkOutDate,
                    "Hotel": "$_id",
                    "Price": "$Price2",
                }
            },
            {"$project": {"_id": 0, "Price2": 0, "count": 0}},
        ]
    )

    lowest_price = 0
    responses = []

    for idx, hotel in enumerate(result):
        if idx == 0:
            lowest_price = hotel["Price"]
        if hotel["Price"] == lowest_price:
            responses.append(hotel)
        else:
            break

    responses = json.dumps(responses)
    responses = json.loads(responses)
    return responses
