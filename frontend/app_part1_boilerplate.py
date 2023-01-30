import os
from flask import Flask
from flask import render_template
from dotenv import load_dotenv

app = Flask(__name__)
load_dotenv()

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/order_food')
def order(item={'name':'hamburger', 'quantity':1}):
    return item

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9000)