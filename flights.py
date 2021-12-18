def get_flights_for_date(client, database, date):
    """
    Get all flight details for a specific date
    param client MongoClient 
    param database Dabase name
    param date in the format 'MM/DD/YY' in string 
    """
    flights = client[database].flights
    results = flights.aggregate([
        {
            '$match' : {
                "Date" : date
            }
        },
        {
            '$project' : {
                "_id" : 0,
                "Flight" : "$Flight_No",
                "Boarding Gate" : "$Gate",
                "Date" : 1,
                "Time" : {
                    '$ifNull' : [ "$Actual_time", "$Scheduled_time" ]
                },
                "Airline" : "$Airline_Name",
                "Status" : 1,
                "Remarks" : 1,
                "Destination" : "$To",
                "Origin" : "$From"
            }
        }, 
        {
            '$set' : {
                "Time": {
                    '$cond': [
                        { #if the time is in the format H:MM it adds a 0 infront 
                            '$eq': [ { '$strLenCP': '$Time' }, 4 ]
                        }, {
                            '$concat': [ '0', '$Time' ]
                        }, '$Time'
                    ]
                }
            }
        }, 
        { #arranges by time
            '$sort': {'Time': 1}
        }
    ])
    
    

    
    return results
def most_available_seats(client, database, destination):
    """
    Get the flight with the most available seats for a certain destination
    param client MongoClient 
    param database Dabase name
    param destination is the airport code where the flight is headed
    """
    flights = client[database].flights
    results = flights.aggregate([
        {
            '$match' : {
                "To" : destination,
                "Status" : "SCHEDULED"
            }
        },
        {
            '$unwind' : "$Passengers"
        },
        {
            '$group' : {
                "_id" : "$Flight_No",
                "Date" : { '$first' : "$Date" },
                "Time" : { '$first' : "$Scheduled_time" },
                "Terminal" : { '$first' : "$Terminal" },
                "Gate" : { '$first' : "$Gate" },
                "Airport_Name" : { '$first' : "$Airport_Name" },
                "Airport_City" : { '$first' : "$Airport_City" },
                "Airport_Country" : { '$first' : "$Airport_Country" },
                "Total_Seats" : { '$first' :  { '$toInt' : "$Aircraft_Capacity" } },
                "Seats_Taken" : { '$sum' : 1 }
            }
        },
        {
            '$project' : {
                "_id" : 0,
                "Seats_Available" : { '$subtract' : ["$Total_Seats" , "$Seats_Taken"] },
                "Flight_Details" : {
                    "Flight_No" : "$_id",
                    "Destination" : { '$concat' : ["$Airport_Name", ", ", "$Airport_City", ", ", "$Airport_Country"] },
                    "Date" : "$Date",
                    "Time" : "$Scheduled_time",
                    "Terminal" : "$Terminal",
                    "Gate" : "$Gate",
                }
            }
        },
        {
            '$setWindowFields': {
                'sortBy' : { "Seats_Available" : -1 },
                'output': {
                    'Ranking' : { '$rank': {} }
                }
            }
        },
        {
            '$match' : { "Ranking" : 1 }
        }
    ])
    
    return results

def runway_queue(client, database, date, runway):
    """
    Display the runway queue for a specific date and runway number based on scheduled time
    param client MongoClient 
    param database Dabase name
    param date in the format 'MM/DD/YY' in string 
    param runway is the runway number in string
    """
    flights = client[database].flights
    results = flights.aggregate([
        {
            '$match': {
                'Date': date, 
                'Runway_No': runway
            }
        }, {
            '$set': {
                'Scheduled_time': {
                    '$cond': [
                        { #if the time is in the format H:MM it adds a 0 infront 
                            '$eq': [ { '$strLenCP': '$Scheduled_time' }, 4 ]
                        }, {
                            '$concat': [ '0', '$Scheduled_time' ]
                        }, '$Scheduled_time'
                    ]
                }
            }
        }, {
            '$sort': {
                'Scheduled_time': 1
            }
        }, {
            '$project': {
                '_id': 0, 
                'Flight_No': 1, 
                'Scheduled_time': 1
            }
        }
    ])
    
    return results
    
def top_countries_of_origin(client, database):
    """
    Display top 10 country of origin of unique passengers passing through LAX
    param client MongoClient 
    param database Dabase name
    """
    flights = client[database].flights
    results = flights.aggregate([
        {
            '$unwind': {
                'path': '$Passengers'
            }
        }, { #gets only unique passengers
            '$group': {
                '_id': {
                    'Last_Name': '$Passengers.Last_Name',
                    'First_Name': '$Passengers.First_Name', 
                    'ID': '$Passengers.ID', 
                    'Country': '$Passengers.Country'
                }
            }
        }, {
            '$group': {
                '_id': '$_id.Country', 
                'Count': { '$sum': 1 }
            }
        }, {
            '$sort': { 'Count': -1 }
        }, {
            '$limit': 10
        }, {
            '$project': {
                '_id': 0, 
                'Citizen_of': '$_id', 
                'Count': 1
            }
        }
    ])
    
    return results

def ranking_by_airline(client, database):
    """
    Display ranking of the airlines’ usage of the airport
    param client MongoClient 
    param database Dabase name
    """
    flights = client[database].flights
    results = flights.aggregate([
        {
            '$group' : {
                "_id" : "$Airline_Name",
                "count" : { '$sum' : 1 }
            }
        },
        {
            '$setWindowFields': {
                'sortBy' : { "count" : -1 },
                'output': {
                    'Ranking' : { '$rank': {} }
                }
            }
        },
        {
            '$project' : {
                "_id" : 0,
                "Airline" : "$_id",
                "Ranking" : 1
            }
        }
    ])
    
    return results

def airline_statistics(client, database):
    """
    Show on-time, delayed, and cancelled percentages of each airline, arranged by highest on-time percentage
    param client MongoClient 
    param database Dabase name
    """
    flights = client[database].flights
    results = flights.aggregate([
        { 
            '$addFields': { 
                'on_time': {
                    '$cond': [ { '$eq': [ '$Remarks', 'ON TIME' ] }, 1, 0 ]
                },
                'delayed': {
                    '$cond': [ { '$eq': [ '$Remarks', 'DELAYED' ] }, 1, 0 ]
                }, 
                'cancelled': {
                    '$cond': [ { '$eq': [ '$Remarks', 'CANCELLED' ] }, 1, 0 ]
                }
            }
        }, {
            '$group': { #counts the total number of on time, delayed, and cancelled flights for each airline
                '_id': '$Airline_Name', 
                'on_time_count': { '$sum': '$on_time' }, 
                'delayed_count': { '$sum': '$delayed' }, 
                'cancelled_count': { '$sum': '$cancelled' }
            }
        }, { #adds a field that is a total of the on time, delayed, and cancelled flights
            '$addFields': {
                'total_count': {
                    '$add': [ '$on_time_count', '$delayed_count', '$cancelled_count' ]
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'Airline': '$_id', 
                'on_time_percentage': { #rounds off the percentage for better display
                    '$round': [ {
                        '$cond': [ { 
                            '$ne': [ '$total_count', 0 ]
                        }, { #gets the percentage
                            '$multiply': [ { '$divide': [ '$on_time_count', '$total_count' ] }, 100 ]
                        }, 0 ]
                    }, 2 ]
                }, 
                'cancelled_percentage': { #rounds off the percentage for better display
                    '$round': [ {
                        '$cond': [ {
                            '$ne': [ '$total_count', 0 ]
                        }, { #gets the percentage
                            '$multiply': [ { '$divide': [ '$cancelled_count', '$total_count' ] }, 100 ]
                        }, 0 ]
                    }, 2 ]
                }, 
                'delayed_percentage': { #rounds off the percentage for better display
                    '$round': [ {
                        '$cond': [ {
                            '$ne': [ '$total_count', 0 ]
                        }, { #gets the percentage
                            '$multiply': [ { '$divide': [ '$delayed_count', '$total_count' ] }, 100 ]
                        }, 0 ]
                    }, 2 ]
                }
            }
        }, { 
            '$sort': {
                'on_time_percentage': -1
            }
        }, {
            '$project': { #adds the percent symbol '%'
                'Airline': '$Airline', 
                'on_time_percentage': {'$concat': [{'$toString' :'$on_time_percentage'}, '%']},
                'cancelled_percentage': {'$concat': [{'$toString' :'$cancelled_percentage'}, '%']},
                'delayed_percentage': {'$concat': [{'$toString' :'$delayed_percentage'}, '%']}
            }
        }
    ])

    return results


def top_destinations(client, database):
    """
    Show top 5 destinations with the most visitors
    param client MongoClient 
    param database Dabase name
    """
    flights = client[database].flights
    results = flights.aggregate([
        {
            '$match' : { #only gets flights leaving LAX
                "Flight_Type" : "DEPARTURE"
            }
        },
        {
            '$unwind' : "$Passengers"
        },
        {
            '$group' : { 
                "_id" : "$To",
                "count" : { '$sum' : 1 }
            }
        },
        {
            '$sort' : { "count" : -1 }
        },
        {
            '$limit' : 5
        },
        {
            '$project' : {
                "_id" : 0,
                "Destination" : "$_id",
                "No_of_Visitors" : "$count"
            }
        }
    ])
    
    return results
    
def passengers_from_state(client, database, origin, date):
    """
    Show all passengers coming from a specific state with a flight at a specific date
    param client MongoClient 
    param database Dabase name
    param origin is the airport code where the flight originated
    """
    flights = client[database].flights
    results = flights.aggregate([
        {
            '$match': {
                'From': origin, 
                'Date': date
            }
        }, {
            '$unwind': {
                'path': '$Passengers'
            }
        }, {
            '$group': {
                '_id': {
                'Flight_No': '$Flight_No', 
                'Date': '$Date'
                }, 
                'Passengers': {
                    '$push': { 
                        '$concat': [ '$Passengers.First_Name', ' ', '$Passengers.Last_Name' ]
                    }
                }
            }
        },
        {
            '$project' : {
                "_id" : 0,
                "Flight_Details" : "$_id",
                "Passengers" : 1
            }
        }
    ])
    
    return results

def top_aircraft(client, database):
    """
    Show most used aircraft type
    param client MongoClient 
    param database Dabase name
    """
    flights = client[database].flights
    results = flights.aggregate([
        {
            '$group' : {
                "_id": "$Aircraft_Type",
                "count": { '$sum' : 1 }
            }  
        },
        {
            '$sort' : {
                "count" : -1
            }
        },
        {
            '$limit' : 1
        },
        {
            '$project' : {
                "_id" : 0,
                "Aircraft_Type" : "$_id",
                "Used" : { '$concat' : [ {'$toString' : "$count" }, " ", "times" ] }
            } 
        }
    ])
    
    return results