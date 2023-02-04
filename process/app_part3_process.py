import os
from dotenv import load_dotenv
import datetime
import pymongo
import json
import asyncio
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError

load_dotenv()
try:
    myclient = pymongo.MongoClient("mongodb://"+ os.getenv('DATABASE_URL') +"/")
    mydb = myclient["resturant"]
    mycol = mydb["orders"]
    print("Connected to DB")
except:
    print("No hosts found")

async def send_delivery(item):
    try:
        memphis = Memphis()
        await memphis.connect(host=os.getenv("MEMPHIS_URL"), username=os.getenv("MEMPHIS_USERNAME"), connection_token=os.getenv("MEMPHIS_CONNECTION_TOKEN"))
        producer = await memphis.producer(station_name=os.getenv("MEMPHIS_DELIVERIES_STATION_NAME"), producer_name="process-workers", generate_random_suffix=True)
        await producer.produce(message=item)

    except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
        print(e)

    finally:
        await memphis.close()

async def main():
    async def process_order(msgs, error, context):
        try:
            for msg in msgs:
                order = json.loads(msg.get_data())
                current_time = datetime.datetime.now()
                print("New order received: ", str(order))
                print("Prepering the dish")
                print("Order is ready for delivery. Updating its DB record")
                order["ready_date"] = current_time.strftime("%m/%d/%Y, %H:%M:%S")
                order["status"] = "delivery"
                await msg.ack()

                try:
                    filter = { '_id': order["_id"] }
                    newvalues = { "$set": { "ready_date": order["ready_date"], 'status': "delivery" } }
                    mycol.update_one(filter, newvalues)
                except:
                    print("DB is not available")

                await send_delivery(order)
                if error:
                    print(error)
        except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
            print(e)
            return
    try:
        memphis = Memphis()
        await memphis.connect(host=os.getenv("MEMPHIS_URL"), username=os.getenv("MEMPHIS_USERNAME"), connection_token=os.getenv("MEMPHIS_CONNECTION_TOKEN"))
        
        consumer = await memphis.consumer(station_name=os.getenv("MEMPHIS_ORDERS_STATION_NAME"), consumer_name="process-worker1", consumer_group="process-workers")
        consumer.consume(process_order)

        # Keep your main thread alive so the consumer will keep receiving data
        await asyncio.Event().wait()
        
    except (MemphisError, MemphisConnectError) as e:
        print(e)
        
    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())