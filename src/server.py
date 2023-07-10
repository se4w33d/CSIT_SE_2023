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
        format_in = date.strftime(parse_in, '%Y-%m-%d')
        format_out = date.strftime(parse_out, '%Y-%m-%d')
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


@app.get("/flight", status_code=200)
def get_flight(departureDate: str, returnDate: str, destination: str):
    
    app.flights = app.database.flights

    departureDate, returnDate = validate_date_format(departureDate, returnDate)

    cities = ["Singapore", destination]
    cities.sort()

    # res = validate_destination(app.flights, {"destcity": destination})
    # if res:
    #     return res

    result = app.flights.aggregate(
        [
            {
                "$match": {
                    "srccity": {"$in": cities},
                    "destcity": {"$in": cities},
                    "date": {
                        "$in": [
                            datetime.fromisoformat(departureDate),
                            datetime.fromisoformat(returnDate),
                        ]
                    },
                }
            },
            {
                "$match": {
                    "$or": [
                        {
                            "$and": [
                                {"srccity": "Singapore"},
                                {"date": datetime.fromisoformat(departureDate)},
                            ]
                        },
                        {
                            "$and": [
                                {"srccity": destination},
                                {"date": datetime.fromisoformat(returnDate)},
                            ]
                        },
                    ]
                }
            },
            {
                "$bucket": {
                    "groupBy": "$srccity",
                    "boundaries": cities,
                    "default": cities[-1],
                    "output": {
                        "count": {"$sum": 1},
                        "flights": {
                            "$push": {
                                "date": "$date",
                                "airline": "$airlinename",
                                "price": "$price",
                            }
                        },
                    },
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "count": 1,
                    "flights": {
                        "$sortArray": {"input": "$flights", "sortBy": {"price": 1}}
                    },
                }
            },
        ]
    )

    if not (result := list(result)):
        return []

    src = dict()
    dest = dict()

    for i in result:
        if i["_id"] == "Singapore":
            src["flights"] = []
            populate_to_src_dest(i, src)
        else:
            dest["flights"] = []
            populate_to_src_dest(i, dest)


    src_dest = list(zip(src["flights"], dest["flights"]))

    lowest_price = src_dest[0][0]["price"] + src_dest[0][1]["price"]
    flag = -1

    for i in src_dest:
        if i[0]["price"] + i[1]["price"] == lowest_price:
            flag += 1

    src_dest = src_dest[:flag+1]

    responses = []

    for i in src_dest:
        flight = dict()
        flight["City"] = destination
        flight["Departure Date"] = departureDate
        flight["Departure Airline"] = i[0]["airline_name"]
        flight["Departure Price"] = i[0]["price"]
        flight["Return Date"] = returnDate
        flight["Return Airline"] = i[1]["airline_name"]
        flight["Return Price"] = i[1]["price"]
        responses.append(flight)

    responses = json.dumps(responses)
    responses = json.loads(responses)
    return (responses)


@app.get("/hotel", status_code=200)
def get_hotel(checkInDate: str, checkOutDate: str, destination: str):
    app.hotels = app.database.hotels

    checkInDate, checkOutDate = validate_date_format(checkInDate, checkOutDate)

    diff = compute_day_difference(checkInDate, checkOutDate)

    # res = validate_destination(app.hotels, {"city": destination})
    # if res:
    #     return res

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
    return (responses)