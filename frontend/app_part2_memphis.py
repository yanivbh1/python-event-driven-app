import os
from flask import Flask
from flask import render_template
from dotenv import load_dotenv
import pymongo
import datetime
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError


app = Flask(__name__)
load_dotenv()
myclient = pymongo.MongoClient("mongodb://"+ os.getenv('DATABASE_URL') +"/")
mydb = myclient["resturant"]
mycol = mydb["orders"]

async def produce_event_to_memphis(item):
    try:
        memphis = Memphis()
        await memphis.connect(host=os.getenv("MEMPHIS_URL"), username=os.getenv("MEMPHIS_USERNAME"), connection_token=os.getenv("MEMPHIS_CONNECTION_TOKEN"))
        producer = await memphis.producer(station_name=os.getenv("MEMPHIS_ORDERS_STATION_NAME"), producer_name="website", generate_random_suffix=True)
        await producer.produce(item)

    except (MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError) as e:
        print(e)
        await memphis.close()

    finally:
        await memphis.close()


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/order_food')
async def order(item={'name':'hamburger', 'quantity':1, 'status': "new_order"}):

    # Decorate the order with random ID and date time

    print("New order received!")
    print(item)
    current_time = datetime.datetime.now()
    item["order_date"] = current_time.strftime("%m/%d/%Y, %H:%M:%S")
    mycol.insert_one(item)

    # Push an order to memphis

    await produce_event_to_memphis(item)

    return item

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)