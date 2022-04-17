import pandas as pd
from flask import Flask,jsonify,request
from connection import  connection

app = Flask(__name__)
object_conn = connection()

@app.route('/display')
def hello_world():
    if request.args.get("customer_id"):
        customer = request.args.get("customer_id")
    else:
        customer = None
    if request.args.get("machine_id"):
        machine = request.args.get("machine_id")
    else:
        machine = None
    if request.args.get("startdate"):
        startdate = request.args.get("startdate")
    else:
        startdate = None
    if request.args.get("enddate"):
        enddate = request.args.get("enddate")
    else:
        enddate = None
    if request.args.get("timeframe"):
        timeframe = request.args.get("timeframe")
    else:
        timeframe = None
    if request.args.get("agg_function"):
        agg_function = request.args.get("agg_function")
    else:
        agg_function =None
    # if request.args.get("range"):
    #     range = request.args.get("range")
    data = object_conn.fetching(customer=customer,machine=machine,enddate=enddate,startdate=startdate)
    data = data.fillna("")
    data = data.set_index('ts')

    if agg_function == "average":
        data = data.resample(f'{timeframe}').mean()
    elif agg_function == "sum":
        data = data.resample(f'{timeframe}').sum()
    elif agg_function == "min":
        data = data.resample(f'{timeframe}').min()
    elif agg_function == "max":
        data = data.resample(f'{timeframe}').max()
    elif agg_function == "count":
        data = data.resample(f'{timeframe}').count()
    else:
        print("select valid agg function")

    data = data.fillna(0)
    data = data['v1'].tolist()
    var = jsonify({'V1':data})
    return var

if __name__ == '__main__':
    app.run(debug=True)