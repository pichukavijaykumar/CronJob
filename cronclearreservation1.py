# psycopg2 is a Python adapter for working with PostgresSQL databases
import psycopg2
from crontab import CronTab
from datetime import datetime, timedelta

# Set up the PostgresSQL connection
try:
    postgres_data = psycopg2.connect(
        host="localhost",
        database="",
        user="postgres",
        password="Vj@13396m042",
        port=5432
    )
except psycopg2.Error as e:
    print("Unable to connect to the database: ", e)
    exit()

# Define the query to select expired reservations
query = """
SELECT DISTINCT sr.reservationIdentifier 
FROM StockReservation sr 
WHERE sr.expireAt < %(currentDateTime)s;
"""

# Define the query to delete expired reservations
delete_query = """
DELETE FROM StockReservation sr 
WHERE sr.reservationIdentifier = %(reservationId)s;
"""

# Get the current date and time
current_datetime = datetime.now()

# Added The 20 minutes from the current date and time to get the expiration time
expiration_datetime = current_datetime + timedelta(minutes=20)

try:
    # Execute the query to select expired reservations
    with postgres_data.cursor() as cur:
        cur.execute(query, {'currentDateTime': expiration_datetime})
        expired_reservations = cur.fetchall()
        print(expired_reservations)

        # Delete expired reservations
        for reservation_id in expired_reservations:
            cur.execute(delete_query, {'reservationId': reservation_id})
            print(reservation_id)

    # Commit the changes to the database
    postgres_data.commit()
    print("Expired reservations deleted successfully!")
except psycopg2.Error as e:
    postgres_data.rollback()
    print("Error occurred while deleting expired reservations: ", e)

# Close the database connection
postgres_data.close()

# Add the Cron Job to schedule the script to run every 20 minutes

# */20 * * * * User\VIJAYKUMAR\Desktop\CronReservationClear\cronclearreservation1.py