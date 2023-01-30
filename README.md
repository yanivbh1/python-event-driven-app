# python-event-driven-app

## Part 1 boilerplate flask app
python3 -m venv .
source bin/activate
python3 -m pip install --upgrade pip
pip install python-dotenv
print(os.getenv('test'))
http://localhost:9000/
https://github.com/naveed125/rabbitmq-job-worker/blob/master/server/Dockerfile

## Part 2: Installing Memphis and push orders
curl -s https://memphisdev.github.io/memphis-docker/docker-compose.yml -o docker-compose.yml && docker compose -f docker-compose.yml -p memphis up

## Part 3: Adding workers or consumers
Consumer 1: saving the record to a db
Consumer 2: process the order + update the DB + notify the customer + move to delivery queue
