import os
import pandas as pd
from wsgiref import simple_server
from flask import Flask, request, render_template, Response
import flask_monitoringdashboard as dashboard
from flask_cors import CORS, cross_origin
from apps.core.config import Config

app = Flask(__name__)
CORS(app)

@app.route('/training', methods=['POST'])
@cross_origin()
def training_route_client():
    """
    * method: training_route_client
    * description: To invoke training route
    * return: None
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * VIJAY-GADRE     06-MAY-2020    1.0      initial creation
    *
    * Parameters
    *   None
    """
    try:
        return Response("Training Successfull!!!")
    except ValueError:
        return Response("Error Occurred! %s " % ValueError)
    except KeyError:
        return Response("Error Occurred! %s " % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s " & e)


if __name__ == '__main__':
    host = '0.0.0.0'
    port = 1096
    httpd = simple_server.make_server(host, port, app)
    httpd.serve_forever()
