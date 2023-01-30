FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "flask", "--app=app_part2_memphis.py", "run", "--port=8000", "--host=0.0.0.0"]