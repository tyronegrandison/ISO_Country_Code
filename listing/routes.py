from flask import Flask, request, jsonify
from flask.ext.sqlalchemy import SQLAlchemy

app = Flask(__name__)
db = SQLAlchemy(app)

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:root@localhost/ISO_Country'

class Listing(db.Model):
	__tablename__ = 'country'
	id = db.Column(db.Integer, primary_key = True)
	name = db.Column(db.String(30))
	iso2 = db.Column(db.String(2))
	iso3 = db.Column(db.String(2))
	code = db.Column(db.String(2))
	status = db.column(db.String(24))

@app.route('/listings/', methods=['GET'])
def listings():
  if request.method == 'GET':
    results = Listing.query.limit(300).offset(0).all()
    json_results = []
    for result in results:
      d = {'Name': result.name,
           'ISO-2': result.iso2,
           'ISO-3': result.iso3,
           'Code': result.code,
           'Status':result.status}
      json_results.append(d)

    return jsonify(items=json_results)


if __name__ == '__main__':
  app.run(debug=True)


