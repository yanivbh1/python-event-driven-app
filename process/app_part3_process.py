import os
from dotenv import load_dotenv
import datetime
import json
import asyncio
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError

load_dotenv()

async def send_delivery(item):
    try:
        memphis = Memphis()
        await memphis.connect(host=os.getenv("MEMPHIS_URL"), username=os.getenv("MEMPHIS_USERNAME"), connection_token=os.getenv("MEMPHIS_CONNECTION_TOKEN"))
        producer = await memphis.producer(station_name=os.getenv("MEMPHIS_DELIVERIES_STATION_NAME"), producer_name="process-workers", generate_random_suffix=True)
        await producer.produce(item)

    except (MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError) as e:
        print(e)

    finally:
        await memphis.close()

async def main():
    async def process_order(msgs, error, context):
        try:
            for msg in msgs:
                order = json.loads(msg.get_data())
                print("New order received: ", str(order))
                print("Prepering the dish")
                file_name = order["order_date"].split(",")[0].replace("/","-")
                record_name = os.getenv('DATABASE_COMPLETED_ORDERS_PATH') + "/" + file_name +'.txt'
                with open(record_name, 'a') as f:
                    f.write(str(order) + "\n")
                    f.close()
                await msg.ack()
                print("Ready to deliver")
                print("Notify the customers that their food is on its way!")
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