import pandas as pd
import singlestoredb as db
from openai import OpenAI
import os

os.environ["OPENAI_API_KEY"]="key"
client = OpenAI()


# Initialize OpenAI API with your API key

# Function to generate embeddings using OpenAI's API
def get_embedding(text, model="text-embedding-3-small"):
   text = text.replace("\n", " ")
   return client.embeddings.create(input = [text], model=model).data[0].embedding

def connector():
    return db.connect(host='', port='3333', user='',
                  password='', database='database_3bc91')

def read_vectors(vector):
    output = []
    try:
        mydb = connector()
        mycursor = mydb.cursor()

        sql = "SELECT product_name, review_title, review_text, dot_product(json_array_pack(%s), vector) as score, JSON_ARRAY_UNPACK(vector) as vector FROM myvectors order by score desc limit 5"
        mycursor.execute(sql, (str(vector)))
        result = mycursor.fetchall()
        for (product_name, review_title, review_text, score, vector) in result:
            output.append({
                #'product_name': product_name,
                'review_title': review_title,
                #'review_text': review_text,
                #'vector': vector,
                'score': score              
            })
        mydb.close()
    except Exception as e:
        print(e)
    return output

def insert_vector(username, name, title, text, vector):
    id = 0
    try:
        mydb = connector()
        mycursor = mydb.cursor()
        sql = 'INSERT INTO myvectors (username, product_name, review_title, review_text, vector) values (%s, %s, %s, %s, JSON_ARRAY_PACK(%s))'
        mycursor.executemany(sql, [(username, name, title, text, str(vector))])
        id = mycursor.lastrowid
        mydb.commit()
    except Exception as e:
        print(e)
        pass
    try:
        mydb.close()
    except:
        pass
    return id
# Data Ingestion
def read_csv(file_path):
    df = pd.read_excel(file_path)
    return df


# Manual Embedding
def create_embeddings(data):
    embeddings = []
    for _, row in data.iterrows():
        combined_text = f"{row['reviews.title']} {row['reviews.text']}"
        embedding = get_embedding(combined_text)
        insert_vector(row['reviews.username'], row['name'], row['reviews.title'], row['reviews.text'], embedding)
        # embedding = generate_embedding(combined_text)
        # embeddings.append(embedding)
    return embeddings

def read_and_store_to_db():
    data = read_csv("customer.xlsx")
    create_embeddings(data)

def search_vector_from_db():
    text= "Best Tablet"
    vector = get_embedding(text)
    print(read_vectors(vector))
if __name__ == "__main__":

    #Following function will read csv and store data with vectors
    #read_and_store_to_db()

    #Following function will help to search the nearest review using vectors
#     try:
#         mydb = connector()
#         mycursor = mydb.cursor()
#         sql = """CREATE TABLE myvectors (
#   product_name text CHARACTER SET utf8 COLLATE utf8_general_ci,
#   review_title text CHARACTER SET utf8 COLLATE utf8_general_ci,
#   review_text text CHARACTER SET utf8 COLLATE utf8_general_ci,
#   vector blob,
#   username text CHARACTER SET utf8 COLLATE utf8_general_ci,
#   SORT KEY __UNORDERED ()
#   , SHARD KEY () 
# ) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES'"""
#         mycursor.execute(sql)
        
#         # result = mycursor.fetchall()
#         # for (product_name, review_title, review_text, score, vector) in result:
#         #     output.append({
#         #         #'product_name': product_name,
#         #         'review_title': review_title,
#         #         #'review_text': review_text,
#         #         #'vector': vector,
#         #         'score': score              
#         #     })
#         # mydb.close()
#     except Exception as e:
#         print(e)
#         pass
#     finally:
#         mydb.close()
    # read_and_store_to_db()
    search_vector_from_db()


    



