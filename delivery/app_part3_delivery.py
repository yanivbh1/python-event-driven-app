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

async def main():
    async def process_delivery(msgs, error, context):
        try:
            for msg in msgs:
                order = json.loads(msg.get_data())
                current_time = datetime.datetime.now()
                print("New delivery received: ", str(order))
                print("Delivering the order")
                print("Updating its DB record")
                order["delivery_date"] = current_time.strftime("%m/%d/%Y, %H:%M:%S")
                order["status"] = "delivered"
                await msg.ack()

                try:
                    filter = { '_id': order["_id"] }
                    newvalues = { "$set": { "delivery_date": order["delivery_date"], 'status': order["status"] } }
                    mycol.update_one(filter, newvalues)
                except:
                    print("DB is not available")

                if error:
                    print(error)
        except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
            print(e)
            return
    try:
        memphis = Memphis()
        await memphis.connect(host=os.getenv("MEMPHIS_URL"), username=os.getenv("MEMPHIS_USERNAME"), connection_token=os.getenv("MEMPHIS_CONNECTION_TOKEN"))
        
        consumer = await memphis.consumer(station_name=os.getenv("MEMPHIS_DELIVERIES_STATION_NAME"), consumer_name="delivery-worker1", consumer_group="delivery-workers")
        consumer.consume(process_delivery)

        # Keep your main thread alive so the consumer will keep receiving data
        await asyncio.Event().wait()
        
    except (MemphisError, MemphisConnectError) as e:
        print(e)
        
    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())