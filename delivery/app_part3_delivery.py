import os
from dotenv import load_dotenv
import json
import asyncio
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError

load_dotenv()

async def main():
    async def deliver_an_order(msgs, error, context):
        try:
            for msg in msgs:
                order = json.loads(msg.get_data())
                print("New order received: ", str(order))
                await msg.ack()
                if error:
                    print(error)
        except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
            print(e)
            return
    try:
        memphis = Memphis()
        await memphis.connect(host=os.getenv("MEMPHIS_URL"), username=os.getenv("MEMPHIS_USERNAME"), connection_token=os.getenv("MEMPHIS_CONNECTION_TOKEN"))
        
        consumer = await memphis.consumer(station_name=os.getenv("MEMPHIS_ORDERS_STATION_NAME"), consumer_name="database-worker1", consumer_group="database-workers")
        consumer.consume(deliver_an_order)

        # Keep your main thread alive so the consumer will keep receiving data
        await asyncio.Event().wait()
        
    except (MemphisError, MemphisConnectError) as e:
        print(e)
        
    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())