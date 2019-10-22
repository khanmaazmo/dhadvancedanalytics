import flask
from flask import request, url_for, jsonify, make_response, Response
import pickle
import pandas as pd
import pyodbc
from flask_restplus import Api, Resource, fields

app = flask.Flask(__name__)
app.config.from_object('config')

if app.config["RUNTYPE"] == 'PROD':
    @property
    def specs_url(self):
        # """Monkey patch for HTTPS"""
        return url_for(self.endpoint('specs'), _external=True, _scheme='https')
    Api.specs_url = specs_url

filename = "finalized_model.pickle"
print(filename)
clf = pickle.load(open(filename, 'rb'))

damfilename = "damchurnmodel.pickle"
print(filename)
dammodel = pickle.load(open(damfilename, 'rb'))

authorizations = {
    'Basic Auth': {
        'type': 'basic',
        'in': 'header',
        'name': 'Authorization'
    },
}

api = Api(app,security='Basic Auth', authorizations=authorizations)
apinamespace = api.namespace('damapis', description='DH AA COE Published Data Science Model APIs for Dubai Asset Management')

a_language = apinamespace.model('Language', {'language': fields.String('The Language.')})
a_damchurnjson = apinamespace.model('damchurnjson', {'htent': fields.Integer('Htenant ID'),'hunit': fields.Integer('Unit ID')})

languages = []
python = {'language': 'Python'}
languages.append(python)

@apinamespace.route('/predictchurnrisk')
class DAMChurnPredictRisk(Resource):
    @apinamespace.expect(a_language)
    def get(self):
        return languages

    @apinamespace.expect(a_damchurnjson)
    def post(self):
        if request.authorization:
            username = request.authorization.username
            password = request.authorization.password
        else:
            return make_response('Basic Authentication not provided', 401, {'WWW-Authenticate': 'Basic-realm="Login Required"'})

        if username != app.config["DAMCHURPRED_USERNAME"] or password != app.config["DAMCHURPRED_PASSWORD"]:
            return make_response('Incorrect Basic Authentication', 401, {'WWW-Authenticate' : 'Basic-realm="Login Required"'})

        content = request.get_json()
        inputdf = pd.io.json.json_normalize(content)
        print(inputdf.count())
        server = app.config["DAMCHURPRED_DBSERVER"]
        database = app.config["DAMCHURPRED_DBNAME"]
        dbusername = app.config["DAMCHURPRED_DBUSER"]
        dbpassword = app.config["DAMCHURPRED_DBPWD"]
        dhserver = app.config["DHMODEL_DBSERVER"]
        dhdatabase = app.config["DHMODEL_DBNAME"]
        dhdbusername = app.config["DHMODEL_DBUSER"]
        dhdbpassword = app.config["DHMODEL_DBPWD"]

        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + dbusername + ';PWD=' + dbpassword)
        cnxndh = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + dhserver + ';DATABASE=' + dhdatabase + ';UID=' + dhdbusername + ';PWD=' + dhdbpassword)
        cursordh = cnxndh.cursor()

        for index, row in inputdf.iterrows():
             htent = float(row['htent'])
             hunit = float(row['hunit'])
             query = "SELECT htent, hunit, preds from dbo.damchurnprediction where htent = " + str(htent) + " and hunit = " + str(hunit)
             df = pd.read_sql(query, cnxn)
             print(df)
             if index == 0:
                 outputdf = df
             else:
                 outputdf = pd.concat([outputdf, df])
             cursordh.execute(
                 "INSERT dbo.modelstats(Vertical, Model, Value, DateofCall, Input1,Input2) VALUES('DAM','ChurnPrediction',90000,GETDATE(),?,?)",
                 htent, hunit)
             cnxndh.commit()

        print(outputdf)
        resp = Response(response=outputdf.to_json(orient='records'),status=200,mimetype="application/json")
        #return outputdf.to_json(orient='records')
        return resp

@apinamespace.route('/leadscore')
class DAMLeadScore(Resource):
    @apinamespace.expect(a_language)
    def get(self):
        return languages

if __name__ == '__main__':
    app.run(debug=True)
