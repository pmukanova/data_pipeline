import csv 
import mysql.connector
import pprint

def read_from_csv(path):
    val = []
    with open(path, newline='') as csvfile:
        ticket_reader = csv.reader(csvfile, delimiter=',')
        for row in ticket_reader:
            val.append(row)
    return val

def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(user="root",
                                            password="password09",
                                            host="localhost",
                                            port="3306",
                                            database="third_party_sales_db")

    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    
    return connection 

def load_third_party(connection, file_path_csv):
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS ticket_sales")
    cursor.execute("CREATE TABLE ticket_sales (ticket_id INT,"
                                            + "trans_date DATE," 
                                            + "event_id INT,"
                                            + "event_name VARCHAR(50),"
                                            + "event_date DATE,"
                                            + "event_type VARCHAR(10),"
                                            + "event_city VARCHAR(20),"
                                            + "customer_id INT,"
                                            + "price DECIMAL,"
                                            + "num_tickets INT)")
    sql = "INSERT INTO ticket_sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city ,customer_id, price, num_tickets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = read_from_csv(file_path_csv)
    
    cursor.executemany(sql, val)
    connection.commit()
    cursor.close()
    return 

def query_popular_tickets(connection):
    #Get the most popular tickets in the past month
    sql_statement = "SELECT event_name FROM ticket_sales Group By event_name limit 3"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

def display_results(file_path_csv):
    
    conn = get_db_connection()
    load_third_party(conn, file_path_csv)
    print("Here are the most popular tickets in the past month:")
    records = query_popular_tickets(conn)

    for i in records:
        print("- " + i[0])

path = input("Please, enter csv file path: ")
display_results(path)