#Author Ken Vellian
#Topic: Database Processing for Large-Scale Analytics
#Date: March 22, 2024

# main() organizes execution of Part 1,2,3

import sqlite3
import json
import requests
import time
import matplotlib.pyplot as plt
import re
import csv


""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# PART 1
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def save_first_n_tweets_to_file_1A(n, url, path_name):
    """
    Part1A: Use python to download tweets from the web and save to a local text file
    
    This function returns total runtime

    Parameters
    ----------
    n : int
        Number of tweets to write the txt file.
    url : str
        URL of OneDayOfTweets.txt.
    path_name : str
        Location to write the file.

    Returns
    -------
    runtime : float
        Total runtime of function.

    """
    start_time = time.time()
    
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  # Ensuring the request succeeds
            line_count = 0  # Keep track of how many lines (tweets) we've processed
            
            with open(path_name, "w") as file:
                for line in response.iter_lines(decode_unicode=True):
                    if line_count < n:  # Only process the first n lines
                        file.write(line + "\n")
                        line_count += 1
                    else:
                        break  # Stop after saving n tweets

    except requests.RequestException as e:
        print(f"Error downloading data: {e}")
    
    runtime = time.time() - start_time
    print(f"\n\nRuntime saving {n} tweets in txt file: {runtime:.2f} seconds\n\n")
    return runtime    
    
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def null_checker(value):
    """
    Converting string "NULL" to or empty string to a None value, else return the original value.
    
    Parameters
    ----------
    value : str
        Original value from data.

    Returns
    -------
    value : str, None
        Transformed value from data.

    """
    if value == "NULL" or value is None or value == "":
        return None
    else:
        return value


def create_tables(db):
    """
    This function creates the 3 tables in the specified databases

    Parameters
    ----------
    db : str
        Database passed from main().
        
    Returns
    -------
    None.

    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # User Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS USER (
        id VARCHAR(70) PRIMARY KEY,
        name VARCHAR(100),
        screen_name VARCHAR(70),
        description VARCHAR(5000),
        friends_count INTEGER
    );""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS GEO (
        geo_id VARCHAR(255) PRIMARY KEY,
        type VARCHAR(50),
        longitude REAL,
        latitude REAL
    );""")

    # Tweet Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS TWEET (
        created_at VARCHAR(70),
        id_str VARCHAR(70) PRIMARY KEY,
        text VARCHAR(300),
        source VARCHAR(500),
        in_reply_to_user_id VARCHAR(70),
        in_reply_to_screen_name VARCHAR(70),
        in_reply_to_status_id VARCHAR(70),
        retweet_count INTEGER,
        contributors VARCHAR(5000),
        user_id VARCHAR(70),
        geo_id VARCHAR(255),
        
        FOREIGN KEY(user_id) REFERENCES USER(id),
        FOREIGN KEY(geo_id) REFERENCES GEO(geo_id)
    );""")
    
    conn.commit()
    conn.close()


def insert_statements():
    """
    Organizing INSERT statements for each table in this function

    Returns
    -------
    insert_user : str
        INSERT statement for USER table.
    insert_geo : str
        INSERT statement for GEO table.
    insert_tweet : str
        INSERT statement for TWEET table.
    """

    insert_user = """
    INSERT OR IGNORE INTO USER 
    (
     id, name, screen_name, description, friends_count
     ) 
    VALUES (?, ?, ?, ?, ?);
    """
    
    insert_geo = """
    INSERT OR IGNORE INTO GEO 
    (
     geo_id, type, longitude, latitude
     ) 
    VALUES (?, ?, ?, ?);

    """
        
    insert_tweet = """
    INSERT OR IGNORE INTO TWEET 
    (
     created_at, id_str, text, source, in_reply_to_user_id, in_reply_to_screen_name, 
     in_reply_to_status_id, retweet_count, contributors, user_id, geo_id
     ) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """  
    return insert_user, insert_geo, insert_tweet


def populate_tables_web_1B(db, insert_user, insert_geo, insert_tweet, url, total_tweets):
    """
    Part 1B: This function populates the specified database with the specified total of tweets from the URL.
    
    This function returns total runtime


    Parameters
    ----------
    db : str
        Database passed from main().
    insert_user : str
        INSERT statements for each table in this function.
    insert_geo : str
        INSERT statements for each table in this function.
    insert_tweet : str
        INSERT statements for each table in this function.
    url : str
        URL of OneDayOfTweets.txt.
    total_tweets : list
        List of count of tweets to populate specific databases.

    Returns
    -------
    runtime : float
        Total runtime of function.

    """
    start_time = time.time()

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    response = requests.get(url, stream=True)
    tweets_processed = 0

    for line in response.iter_lines(decode_unicode=True):
        if tweets_processed >= total_tweets:
            break

        if line.strip():
            try:
                tweet = json.loads(line)
                user = tweet.get("user", {})
                
                # Insert USER data
                cursor.execute(insert_user, (
                    null_checker(user.get("id_str")),
                    null_checker(user.get("name")),
                    null_checker(user.get("screen_name")),
                    null_checker(user.get("description")),
                    null_checker(user.get("friends_count"))
                ))
                
                # Initialize geo_id as None
                geo_id = None
                
                # Extract GEO data if available
                geo = tweet.get("geo")
                if geo and "coordinates" in geo and all(isinstance(coord, (int, float)) for coord in geo["coordinates"]):
                    longitude, latitude = geo["coordinates"]
                    # Create a unique geo_id from longitude and latitude
                    geo_id = f"{longitude}_{latitude}"
                    cursor.execute(insert_geo, (
                        geo_id,
                        "Point",  # Assuming "type" is always "Point"
                        longitude,
                        latitude
                    ))
                
                # Insert TWEET data, including geo_id if available
                cursor.execute(insert_tweet, (
                    null_checker(tweet.get("created_at")),
                    null_checker(tweet.get("id_str")),
                    null_checker(tweet.get("text")),
                    null_checker(tweet.get("source")),
                    null_checker(tweet.get("in_reply_to_user_id_str")),
                    null_checker(tweet.get("in_reply_to_screen_name")),
                    null_checker(tweet.get("in_reply_to_status_id_str")),
                    null_checker(tweet.get("retweet_count")),
                    json.dumps(tweet.get("contributors")),
                    user.get("id_str"),
                    geo_id
                ))

                tweets_processed += 1
                
            except json.JSONDecodeError:
                # print(f"Error decoding JSON for tweet - Line skipped.")
                continue

    conn.commit()
    conn.close()
    
    runtime = time.time() - start_time

    print(f"Finished loading data. Runtime: {runtime:.2f} seconds\n\n")
    return runtime

    
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


def populate_tables_txt_1C(db, insert_user, insert_geo, insert_tweet, path_name, total_tweets):
    """
    Part 1C: This function populates the database from the saved txt file
    
    This function returns total runtime


    Parameters
    ----------
    db : str
        Database passed from main().
    insert_user : str
        INSERT statements for each table in this function.
    insert_geo : str
        INSERT statements for each table in this function.
    insert_tweet : str
        INSERT statements for each table in this function.
    path_name : str
       Location of file to read from.
    total_tweets : list
        List of count of tweets to populate specific databases.

    Returns
    -------
    runtime : float
        Total runtime of function.

    """
    start_time = time.time()
    tweets_processed = 0
    errors_encountered = 0

    with sqlite3.connect(db) as conn, open(path_name, "r", encoding="utf-8") as file:
        cursor = conn.cursor()

        for line in file:
            if tweets_processed >= total_tweets:
                break  # Stop if the desired number of tweets have been processed

            try:
                tweet = json.loads(line)
                user = tweet["user"]

                # Using null_checker to handle possible NULL values
                user_tuple = (
                    null_checker(user["id_str"]),
                    null_checker(user["name"]),
                    null_checker(user["screen_name"]),
                    null_checker(user["description"]),
                    null_checker(user["friends_count"]),
                )
                cursor.execute(insert_user, user_tuple)

                geo = tweet.get("geo")
                geo_id = None
                if geo:
                    # Assuming geo information is properly formatted as a dictionary with "coordinates"
                    longitude, latitude = geo["coordinates"]
                    geo_id = f"{longitude}_{latitude}"
                    geo_tuple = (geo_id, "Point", longitude, latitude)
                    cursor.execute(insert_geo, geo_tuple)

                tweet_tuple = (
                    null_checker(tweet["created_at"]),
                    null_checker(tweet["id_str"]),
                    null_checker(tweet["text"]),
                    null_checker(tweet["source"]),
                    null_checker(tweet.get("in_reply_to_user_id_str")),
                    null_checker(tweet.get("in_reply_to_screen_name")),
                    null_checker(tweet.get("in_reply_to_status_id_str")),
                    null_checker(tweet["retweet_count"]),
                    json.dumps(tweet.get("contributors", [])),  # Defaulting to empty list if missing
                    user["id_str"],
                    geo_id,
                )
                cursor.execute(insert_tweet, tweet_tuple)

                tweets_processed += 1
            except json.JSONDecodeError:
                # print(f"Error decoding JSON for tweet - Line skipped.")
                errors_encountered += 1
                continue
    
    conn.commit()
    conn.close()    
    runtime = time.time() - start_time

    print(f"Finished loading data. Runtime: {runtime:.2f} seconds. Processed: {tweets_processed}, Errors: {errors_encountered}\n\n")
    return runtime


def batch_insert_data_1D(db, insert_user, insert_geo, insert_tweet, path_name, total_tweets, batch_size):
    """
    Part 1D: This function populates the database from the file in batches
    
    This function returns total runtime


    Parameters
    ----------  
    db : str
        Database passed from main().
    insert_user : str
        INSERT statements for each table in this function.
    insert_geo : str
        INSERT statements for each table in this function.
    insert_tweet : str
        INSERT statements for each table in this function.
    path_name : str
       Location of file to read from.
    total_tweets : list
        List of count of tweets to populate specific databases.
    batch_size : int
        Number of rows inserted per batch.
        
    Returns
    -------
    runtime : float
        Total runtime of function.
    """
    start_time = time.time()
    users_batch, geo_batch, tweets_batch = [], [], []
    errors_encountered = 0
    tweets_processed = 0

    with sqlite3.connect(db) as conn, open(path_name, "r", encoding="utf-8") as file:
        cursor = conn.cursor()

        for line in file:
            if tweets_processed >= total_tweets:
                break
            try:
                tweet = json.loads(line)
                user = tweet["user"]

                # Prepare and append data for batch insertion
                user_tuple = (null_checker(user["id_str"]), 
                              null_checker(user["name"]), 
                              null_checker(user["screen_name"]), 
                              null_checker(user["description"]), 
                              null_checker(user["friends_count"]))
                users_batch.append(user_tuple)

                geo = tweet.get("geo")
                geo_id = None
                if geo:
                    longitude, latitude = geo["coordinates"]
                    geo_id = f"{longitude}_{latitude}"
                    geo_tuple = (geo_id, 
                                 "Point", 
                                 longitude, 
                                 latitude)
                    geo_batch.append(geo_tuple)

                tweet_tuple = (null_checker(tweet["created_at"]), 
                               null_checker(tweet["id_str"]), 
                               null_checker(tweet["text"]), 
                               null_checker(tweet["source"]), 
                               null_checker(tweet.get("in_reply_to_user_id_str")), 
                               null_checker(tweet.get("in_reply_to_screen_name")), 
                               null_checker(tweet.get("in_reply_to_status_id_str")), 
                               null_checker(tweet["retweet_count"]), 
                               json.dumps(tweet.get("contributors")), 
                               user["id_str"], 
                               geo_id)
                tweets_batch.append(tweet_tuple)

                tweets_processed += 1

                # Check and execute batch insertion
                if len(users_batch) >= batch_size or len(tweets_batch) >= batch_size:
                    cursor.executemany(insert_user, users_batch)
                    cursor.executemany(insert_geo, geo_batch)
                    cursor.executemany(insert_tweet, tweets_batch)
                    users_batch, geo_batch, tweets_batch = [], [], []  # Reset batches

            except json.JSONDecodeError:
                # print(f"Error decoding JSON for tweet - Line skipped.")
                errors_encountered += 1

        # Insert any remaining batches
        if users_batch or geo_batch or tweets_batch:
            cursor.executemany(insert_user, users_batch)
            cursor.executemany(insert_geo, geo_batch)
            cursor.executemany(insert_tweet, tweets_batch)
            
    conn.commit()
    conn.close()
    runtime = time.time() - start_time
    
    print(f"Finished loading data. Runtime: {runtime:.2f} seconds. Processed: {tweets_processed}, Errors: {errors_encountered}\n\n")
    return runtime


def execute_and_report_queries(db, queries):
    """
    Part 1B, 1C, and 1D
    
    Part 2A
    
    
    This function takes in multiple queries that are organized in a dictionary
    then executes the queries and returns their individual runtimes and sum of runtimes 
    of all the queries passed from the dictionary.
    
    The key is the description of the query, the value is the SQL query

    Parameters
    ----------
    db : str
        Database passed from main().
    queries : dictionary
        Dictionary of queries.

    Returns
    -------
    runtime : float
        returns the runtime of the.

    """
    
    total_start_time = time.time()

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    for description, query in queries.items():
        start_time = time.time()
        cursor.execute(query)
        # Commit after executing a CREATE TABLE statement
        if "CREATE TABLE" or "INSERT" or "DELETE" or "UPDATE" in query.upper():
            conn.commit()
            print(f"{description} - Completed.")
        else:
            # For SELECT statements or others that return data
            result = cursor.fetchone() if cursor.description else None
            if result:
                print(f"{description}: {result[0]}")
        end_time = time.time()
        print(f"{description} - Runtime: {end_time - start_time:.4f} seconds.\n")

    total_end_time = time.time()
    runtime = total_end_time - total_start_time
    print(f"Total Runtime for all queries: {runtime:.4f} seconds.\n")

    conn.commit()
    conn.close()    
    return runtime


def plot_runtime_populate_1E(runtime_results):
    """
    Part 1E: Create plot of runtime results from 1A, 1B, 1C, and 1D
    
    The lineplot utilizes log scale on the y axis to visualize the data better.

    Parameters
    ----------
    runtime_results : dict
        Dictionary of runtimes to plot Part 1A, 1B, 1C, and 1D.

    Returns
    -------
    None.

    """
    method_titles = ["1A: Write Data from Web to TXT File", 
                     "1B: Insert Data from Web", 
                     "1C: Insert Data from TXT File", 
                     "1D: Insert Batches of Data from TXT File"]
    
    plt.figure(figsize=(10, 6))
    
    for method in method_titles:
        x = list(runtime_results.keys())
        y = [runtime_results[num][method] for num in x]
        plt.plot(x, y, label=method, marker="o")
    
    plt.suptitle("Runtime Comparison: Populating Tweet Data in Different Databases")
    plt.title("Testing various scenarios")
    plt.xlabel("Number of Tweets")
    plt.ylabel("Runtime (seconds) (Log Scale)")
    plt.yscale("log")
    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    plt.grid(False)
    plt.savefig("1E_plot_runtime_populate.png", dpi = 300, bbox_inches = "tight")
    plt.show()



""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# PART 2
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def execute_query_iterations_2B(db, query, iteration_counts):
    """
    Part 2B: This function executes the SQL query for average geo location per user multiple times
    and returns the runtime.
    
    It stores the results of every iteration runtime in a dictionary and returns it.

    Parameters
    ----------
    db : str
        Database passed from main().
    query : str
        Query passed from main().
    iteration_counts : int
        Total iterations to run the query.

    Returns
    -------
    runtime_results : dict
        Holds the results of every iteration runtime.

    """
    runtime_results = {}
    for count in iteration_counts:
        start_time = time.time()
        with sqlite3.connect(db) as conn:
            for count in iteration_counts:
                conn.execute(query)
        total_runtime = time.time() - start_time
        runtime_results[count] = total_runtime
        print(f"Total runtime for {count} iterations: {total_runtime:.4f} seconds.")
        print(f"Average runtime per iteration: {total_runtime / count:.4f} seconds.\n")
    return runtime_results


def execute_python_iterations_2C(path_name, iteration_counts):
    """
    Part 2C: This function creates query2A using Python (without SQL)
    
    It stores the results of every iteration runtime in a dictionary and returns it.


    Parameters
    ----------
    path_name : str
        Location to write the file.
    iteration_counts : int
        Total iterations to run the query.

    Returns
    -------
    runtime_results : dict
        Holds the results of every iteration runtime.

    """
    
    runtime_results = {}  # Initialize a dictionary to store total runtimes per iteration count

    for count in iteration_counts:
        start_total_time = time.time()
        user_geo_data = {}

        for iterations in range(count):
            with open(path_name, "r", encoding="utf-8") as file:
                for line in file:
                    try:
                        tweet = json.loads(line)
                        geo = tweet.get("geo")
                        if geo and "coordinates" in geo and all(isinstance(coord, (float, int)) for coord in geo["coordinates"]):
                            user_id = tweet["user"]["id_str"]
                            longitude, latitude = geo["coordinates"]

                            if user_id not in user_geo_data:
                                user_geo_data[user_id] = {"longitude": [], "latitude": []}
                            
                            user_geo_data[user_id]["longitude"].append(longitude)
                            user_geo_data[user_id]["latitude"].append(latitude)

                    except json.JSONDecodeError:
                        # print(f"Error decoding JSON for tweet - Line skipped.")
                        continue

        # After processing all tweets for current iteration count
        end_total_time = time.time()
        total_runtime = end_total_time - start_total_time

        # Store the total runtime for the current iteration count in the dictionary
        runtime_results[count] = total_runtime

        print(f"Total runtime for {count} iterations: {total_runtime:.4f} seconds.")
        print(f"Average runtime per iteration: {total_runtime / count:.4f} seconds.\n")

    # Return the dictionary containing total runtimes per iteration count
    return runtime_results



def execute_regex_iterations_2E(path_name, iteration_counts):
    """
    Part 2E and 2F: This function creates query2A using regular expressions

    It stores the results of every iteration runtime in a dictionary and returns it.

    Parameters
    ----------
    path_name : str
        Location to write the file.
    iteration_counts : int
        Total iterations to run the query.

    Returns
    -------
    runtime_results : dict
        Holds the results of every iteration runtime.

    """
    # Part 2E: Compiling regex patterns for user ID and geo coordinates
    user_id_pattern = re.compile(r'"id_str": "(\d+)"')
    geo_pattern = re.compile(r'"coordinates":\[\s*(\d+.\d+),\s*(\d+.\d+)\s*\]')

    runtime_results = {}

    for count in iteration_counts:
        start_time = time.time()
        user_location_data = {}  # Dictionary to store user location data

        for iterations in range(count):
            with open(path_name, "r", encoding="utf-8") as file:
                for line in file:
                    user_id_match = user_id_pattern.search(line)
                    geo_match = geo_pattern.search(line)
                    if user_id_match and geo_match:
                        user_id = user_id_match.group(1)
                        longitude, latitude = geo_match.groups()

                        if user_id not in user_location_data:
                            user_location_data[user_id] = {"longitude": [], "latitude": []}

                        user_location_data[user_id]["longitude"].append(float(longitude))
                        user_location_data[user_id]["latitude"].append(float(latitude))

        # Summarize location data if necessary, here we"re just tracking runtime
        total_runtime = time.time() - start_time
        runtime_results[count] = total_runtime
        print(f"Total runtime for {count} iterations: {total_runtime:.4f} seconds.")
        print(f"Average runtime per iteration: {total_runtime / count:.4f} seconds.\n")


    # Return the dictionary containing total runtimes per iteration count
    return runtime_results


def plot_runtime_distributions_2G(runtime_results_iterations):
    """
    Part 2G: This function plots the results of Part 2B, 2D, and 2F

    Parameters
    ----------
    runtime_results_iterations : dict
        Dictionary of runtime results across different tests and different iterations.

    Returns
    -------
    None.

    """
    plt.figure(figsize=(10, 6))
    iteration_counts = sorted(runtime_results_iterations.keys())
    methods = list(runtime_results_iterations[next(iter(iteration_counts))].keys())
    
    for method in methods:
        runtimes = [runtime_results_iterations[count][method] for count in iteration_counts]
        plt.plot(iteration_counts, runtimes, marker="o", label=method)
        
    plt.suptitle("Runtime Comparison: Iterations Across Different Tests 5x and 20x")
    plt.title("Finding Average Longitude and Latitude Value for Each User ID")
    plt.xlabel("Number of Iterations")
    plt.ylabel("Runtime (seconds) (Log Scale)")
    plt.yscale("log")
    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    plt.grid(False)
    plt.savefig("2G_plot_runtime_distributions.png", dpi=300, bbox_inches="tight")
    plt.show()


""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# PART 3
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def export_tables_to_json_3B(db, json_path_name):
    """
    Part 3B: This function exports the contents of 
    1) the Tweet table and 2) new table from 3-a into a new JSON file

    Parameters
    ----------
    db : str
        Database passed from main().
    json_path_name : str
        Location to save JSON file to.

    Returns
    -------
    None.

    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # Dictionary to store the json_data from both tables
    json_data = {
        "tweets": [],
        "All_Tweets_Joined_MV": []
    }

    # Fetching and processing json_data from the Tweet table
    cursor.execute("SELECT * FROM TWEET")
    tweets = cursor.fetchall()
    for tweet in tweets:
        json_data["tweets"].append({
            "created_at": tweet[0],
            "id_str": tweet[1],
            "text": tweet[2],
            # Add other fields from the Tweet table here
        })

    # Selecting all the json_data from the All_Tweets_Joined_MV table
    cursor.execute("SELECT * FROM All_Tweets_Joined_MV")
    All_Tweets_Joined_MV = cursor.fetchall()
    
    # Using the actual column names from All_Tweets_Joined_MV table
    for tweet in All_Tweets_Joined_MV:
        json_data["All_Tweets_Joined_MV"].append({
            "created_at": tweet[0],
            "user_name": tweet[11],  # "user_name" is the 12th column in All_Tweets_Joined_MV table
            "screen_name": tweet[12],
            "user_description": tweet[13],
            "friends_count": tweet[14],
            "geo_type": tweet[15],
            "longitude": tweet[16],
            "latitude": tweet[17],
        })

    conn.close()

    # Writing the json_data to a JSON file
    with open(json_path_name, "w", encoding="utf-8") as file:
        json.dump(json_data, file, ensure_ascii=False, indent=4)

    print(f"JSON data exported to {json_path_name}\n")


def export_tables_to_csv_3C(db, tweet_csv_path_name, All_Tweets_Joined_MV_csv_path_name):
    """
    Part 3C: This function exports the contents of 
    1) the Tweet table and 2) your table from 3-a into .csv files

    Parameters
    ----------
    db : str
        Database passed from main().
    tweet_csv_path : str
        Location to save tweet_csv_path file to.
    All_Tweets_Joined_MV_csv_path : str
        Location to save All_Tweets_Joined_MV_csv_path file to.

    Returns
    -------
    None.

    """
    # Connect to the SQLite json_database
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # Export the Tweet table to a CSV file
    cursor.execute("SELECT * FROM TWEET")
    with open(tweet_csv_path_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerows(cursor.fetchall())

    # Export the All_Tweets_Joined_MV table to a CSV file
    cursor.execute("SELECT * FROM All_Tweets_Joined_MV")
    with open(All_Tweets_Joined_MV_csv_path_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerows(cursor.fetchall())

    # Close the json_database connection
    conn.close()

    print(f"Tweet table CSV exported to {tweet_csv_path_name}\n")
    print(f"All_Tweets_Joined_MV table CSV exported to {All_Tweets_Joined_MV_csv_path_name}\n")


""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def main():
    """
    This main() organizes the execution of Part 1,2,3

    Returns
    -------
    None.

    """
    url = "http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt"
    
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # Part 1
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    print("--------------- PART 1 ---------------")
    # Scenarios for the number of tweets
    tweet_counts = [110000, 550000]
    batch_size = 2000

    runtime_results = {}
    
    # Insert statements for database operations
    insert_user, insert_geo, insert_tweet = insert_statements()
    
    # Queries for counting distinct values in each table
    queries_COUNT_DISTINCT = {
        "User Table Row Count": "SELECT COUNT(DISTINCT id) AS user_count FROM USER",
        "Geo Table Row Count": "SELECT COUNT(DISTINCT geo_id) AS geo_count FROM GEO",
        "Tweet Table Row Count": "SELECT COUNT(DISTINCT id_str) AS tweet_count FROM TWEET"
    }

    # Processing tweets and storing runtimes for different data ingestion methods
    for number_of_tweets in tweet_counts:
        path_name = f"first_{number_of_tweets}_tweets.txt"
        
        # Part 1A: Saving tweets to file and calculating runtime
        runtime_to_file = save_first_n_tweets_to_file_1A(number_of_tweets, url, path_name)

        # Part 1B: Direct processing from the web
        db_web = f"tweets_web_{number_of_tweets}.db"
        create_tables(db_web)
        # Calculating run time of populating database from web
        runtime_from_web = populate_tables_web_1B(db_web, insert_user, insert_geo, insert_tweet, url, number_of_tweets)
    
        execute_and_report_queries(db_web, queries_COUNT_DISTINCT) # Calculating runtime of queries_COUNT_DISTINCT on Part 1B
 
        
        # Part 1C: Processing from the saved TXT file
        db_txt = f"tweets_txt_{number_of_tweets}.db"
        create_tables(db_txt)
        
        # Calculating run time of populating database from txt file
        runtime_from_txt = populate_tables_txt_1C(db_txt, insert_user, insert_geo, insert_tweet, path_name, number_of_tweets)
        
        execute_and_report_queries(db_txt, queries_COUNT_DISTINCT) # Calculating runtime of queries_COUNT_DISTINCT on Part 1C


        # Part 1D: Batch processing
        db_batch = f"tweets_batch_{number_of_tweets}.db"
        create_tables(db_batch)
        
        # Calculating run time of populating database from txt file in batches
        runtime_batch = batch_insert_data_1D(db_batch, insert_user, insert_geo, insert_tweet, path_name, number_of_tweets, batch_size)
        
        execute_and_report_queries(db_batch, queries_COUNT_DISTINCT) # Calculating runtime of queries_COUNT_DISTINCT on Part 1D

        
        # Storing runtimes of each test in a dictionary for plotting
        # Key is the test. Value is the runtime
        runtime_results[number_of_tweets] = {
            "1A: Write Data from Web to TXT File": runtime_to_file,
            "1B: Insert Data from Web": runtime_from_web,
            "1C: Insert Data from TXT File": runtime_from_txt,
            "1D: Insert Batches of Data from TXT File": runtime_batch
        }
        
    # Part 1E: Plotting the runtimes
    plot_runtime_populate_1E(runtime_results)
    
    
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # Part 2
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    print("\n\n--------------- PART 2 ---------------")

    # Part 2A: SQL query to find the average longitude and latitude value for each user ID.
    query2A = """
    SELECT user_id, AVG(longitude) AS avg_longitude, AVG(latitude) AS avg_latitude
    FROM Tweet INNER JOIN Geo ON Tweet.geo_id = Geo.geo_id
    GROUP BY user_id;
    """
    
    iteration_counts = [5, 20] # list to store iteration counts
    
    path_name = "first_550000_tweets.txt" # File loaction
    db_web = "tweets_web_550000.db" # Directly passing the larger DB for Part 2
    runtime_results_iterations = {} # Dictionary to store results
    
    for count in iteration_counts:
        
        # Part 2B
        runtime_results2B = execute_query_iterations_2B(db_web, query2A, [count])[count]
        
        # Part 2C and 2D
        runtime_results2D = execute_python_iterations_2C(path_name, [count])[count]
        
        # Part 2E and 2F
        runtime_results2F = execute_regex_iterations_2E(path_name, [count])[count]
        
        # Storing the numeric runtime values directly in this dictionary for plotting
        runtime_results_iterations[count] = {
            "2B: SQL Query from Database": runtime_results2B,
            "2D: Python from TXT File": runtime_results2D,
            "2F: Regex from TXT File": runtime_results2F
        }
        
        # Part 2G plotting the results of runtimes
        plot_runtime_distributions_2G(runtime_results_iterations)
        

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # Part 3
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    print("\n\n--------------- PART 3 ---------------")

    # Part 3A: Define the query to create a new table based on the join of all 3 tables
    # Using LEFT JOIN to include all empty geos as
    query_joined_table_mv = """
    CREATE TABLE IF NOT EXISTS All_Tweets_Joined_MV AS
    SELECT 
        TWEET.*,
        USER.name AS user_name, 
        USER.screen_name, 
        USER.description AS user_description, 
        USER.friends_count,
        GEO.type AS geo_type, 
        GEO.longitude, 
        GEO.latitude
    FROM TWEET
    LEFT JOIN USER ON TWEET.user_id = USER.id
    LEFT JOIN GEO ON TWEET.geo_id = GEO.geo_id;
    """
    
    # Dictionary to pass queries to execute_and_report_queries()
    queries_part3A = {
        "Create Joined Table": query_joined_table_mv,
        "Count Rows in Joined Table": "SELECT COUNT(*) FROM All_Tweets_Joined_MV;"
    }
    
    db_web = "tweets_web_550000.db" # Directly passing the larger DB for Part 3
    
    # Part 3A: Executing and reporting results of Part 3A
    execute_and_report_queries(db_web, queries_part3A)
    
    # Part 3B: Exporting contents of 1) Tweet table, and 2) Joined Table from 3A into new JSON file
    json_path_name = "exported_tweets_data_all_tweets_table.json"
    export_tables_to_json_3B(db_web, json_path_name)

    # Part 3C: Exporting contents of 1) Tweet table, and 2) Joined Table from 3A into new CSV files     
    tweet_csv_path_name = "exported_tweets.csv"
    All_Tweets_Joined_MV_csv_path_name = "exported_All_Tweets_Joined_MV.csv"
    export_tables_to_csv_3C(db_web, tweet_csv_path_name, All_Tweets_Joined_MV_csv_path_name)
    
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

main()
