import flights as fn
from pymongo import MongoClient
from pprint import pprint
    
def user_input(val):
    """
    Interprets the user input and assigns the corresponding function to be run
    param val accepts '1', '2', '3', ... '9'
    """
    switcher={
        '1':fn.get_flights_for_date(client, database, date1),
        '2':fn.most_available_seats(client, database, destination),
        '3':fn.runway_queue(client, database, date2, runway),
        '4':fn.top_countries_of_origin(client, database),
        '5':fn.ranking_by_airline(client, database),
        '6':fn.airline_statistics(client, database),
        '7':fn.top_destinations(client, database),
        '8':fn.passengers_from_state(client, database, origin, date3),
        '9':fn.top_aircraft(client, database)
    }
    return switcher.get(val)

def DisplayMenu():
    """
    Prints the display menu
    """
    
    print("""
    ------------------------------------------------------------------------------------------------------------
    
    What do you want to do? 
    1. Get all flight details for a specific date
    2. Get the flight with the most available seats for a certain destination
    3. Display the runway queue for a specific date and runway number based on scheduled time
    4. Display top 10 country of origin of unique passengers passing through LAX
    5. Display ranking of the airlines’ usage of the airport
    6. Show on-time, delayed, and cancelled percentages of each airline, arranged by highest on-time percentage
    7. Show 5 destinations with the most visitors
    8. Show all passengers coming from a specific state with a flight at a specific date
    9. Show most used aircraft type
    
    Type any number. Other characters will exit the program
    
    ------------------------------------------------------------------------------------------------------------
    """)



if __name__ == '__main__':
    client = MongoClient('18.205.159.250', 27017)
    database = "sample"
    
    #these are the default values used, which can be changed: 
    date1 = "12/20/21"
    destination = "CLT"
    date2 = "12/23/21"
    runway = "2"
    origin = "MIA"
    date3 = "12/23/21"
    
    val =1                                   #initializatiom
    while int(val) in range(1,10):
        DisplayMenu()                        #prints the display menu 
        val = input("Enter number only: ")   #gets the input 
        if val not in ["1","2","3","4","5","6","7","8","9"]:
            break
        # if int(val) not in range(1,10) then program will exit
        flights = user_input(val)
        for flight in flights:
            pprint(flight)
            input("Press Enter to continue...")   
            print("")
print('bye!')







