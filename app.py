# UT-TOR-DATA-PT-01-2020-U-C Week 10 Assignment
# SQLAlchemy Challenge
# Step 2: Climate App
# (c) Boris Smirnov

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc

from flask import Flask, jsonify

import datetime as dt

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
AutomapBase = automap_base()
# reflect the tables
AutomapBase.prepare(engine, reflect=True)
# Save references to each table
Station = AutomapBase.classes.station
Measurement = AutomapBase.classes.measurement


#################################################
# Retrieving globally used data bits
#################################################
session = Session(engine)
# get the first date we have data in the database
first_date = session.query(func.min(Measurement.date)).scalar()
# get the last date we have data in the database
last_date = session.query(func.max(Measurement.date)).scalar()
# calculate the date 1 year before the last data point in the database
year_before = (dt.date(*[int(d) for d in last_date.split('-')]) - dt.timedelta(days=365)).isoformat()
# the most active station for the last year of data
most_active_station_id = session.query(Measurement.station, func.count(Measurement.station).label('measure counts')).\
    filter(Measurement.date > year_before).\
    group_by(Measurement.station).\
    order_by(desc('measure counts')).first()[0]
# end data session for now
session.close()


#################################################
# Flask Setup
#################################################
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    res = """
    <h1>Available Routes:</h1>
    <ul>
        <li>Maximum precipitation values for every day of the past year</li>
        <li style="list-style-type:none">
            <ul><li><a href='/api/v1.0/precipitation'>/api/v1.0/precipitation</a></li></ul>
        </li>

        <li>List of stations from the dataset</li>
        <li style="list-style-type:none">
            <ul><li><a href='/api/v1.0/stations'>/api/v1.0/stations</a></li></ul>
        </li>
        
        <li>One year of temperature observations of the most active station</li>
        <li style="list-style-type:none">
            <ul><li><a href='/api/v1.0/tobs'>/api/v1.0/tobs</a></li></ul>
        </li>
        
        <li>Return minimum, average and maximum temperature for a given range of dates<br>
            Use URLs in the form <code>/api/v1.0/&lt;start_date&gt;</code> or <code>/api/v1.0/&lt;start_date&gt;/&lt;end_date&gt;</code><br>
        </li>
        <li style="list-style-type:none">
            <ul><li><a href='/api/v1.0/'>/api/v1.0/</a></li></ul>
        </li>
    </ul>"""

    return res


@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)

    # query maximum precipitation values for every day of the past year
    prcp_q = session.query(Measurement.date, func.max(Measurement.prcp)).\
                filter(Measurement.date > year_before).\
                group_by(Measurement.date).\
                order_by(Measurement.date).all()

    session.close()

    # convert query result to a dictionary date:prcp
    res_dict = {}
    for row in prcp_q:
        res_dict[row[0]] = row[1]

    return jsonify(res_dict)


@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)

    stations = session.query(Station).all()

    session.close()

    res_lst = []
    # create a dictionary from the row data and append it to a list of stations
    for station in stations:
        station_dict = {}
        station_dict['station'] = station.station
        station_dict['name'] = station.name
        station_dict['latitude'] = station.latitude
        station_dict['longitude'] = station.longitude
        station_dict['elevation'] = station.elevation
        res_lst.append(station_dict)

    return jsonify(res_lst)


@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    station_q = session.query(Station).filter_by(station=most_active_station_id).first()

    tobs_q = session.query(Measurement.date, Measurement.tobs).\
                filter_by(station=most_active_station_id).\
                filter(Measurement.date > year_before).\
                order_by(Measurement.date).all()

    session.close()

    res_dict = {
        'station': station_q.station,
        'name': station_q.name,
        'latitude': station_q.latitude,
        'longitude': station_q.longitude,
        'elevation': station_q.elevation,
        'tobs': {}
    }

    for date, tobs in tobs_q:
        res_dict['tobs'][date] = tobs

    return jsonify(res_dict)


@app.route("/api/v1.0/")
def temp_stub():
    res = """
            <h2>Return minimum, average and maximum temperature for a given range of dates</h2>
            Add to the URL start date or start date and end date in the form <code>YYYY-MM-DD</code>:
            <code>/api/v1.0/&lt;start_date&gt;</code> or <code>/api/v1.0/&lt;start_date&gt;/&lt;end_date&gt;</code><br>
            For example: <a href='/api/v1.0/2017-07-23'>One month from 2017-07-23</a>
            or <a href='/api/v1.0/2016-12-01/2016-12-31'>December 2016</a>
        """
    return(res)


# perform actual database query for @app.route("/api/v1.0/<start_date>") and @app.route("/api/v1.0/<start_date>/<end_date>")
def query_db(start_date, end_date):
    # Parameter checking

    # we work only with string representation of dates in the form YYYY-MM-DD
    if (type(start_date) != str) or (type(end_date) != str):
        return {'error': 'Internal server error'} # shouldn't happen, I use only strings here

    # checking the date format
    try:
        dt.date(*[int(s) for s in start_date.split('-')])
        dt.date(*[int(s) for s in end_date.split('-')])
    except:
        return {'error': 'Invalid date format. Please use YYYY-MM-DD'}

    # checking that values are more or less sane
    if start_date > end_date:
        return {'error': f"Invalid interval: start={start_date} end={end_date}"}
    if end_date > last_date:
        end_date = last_date
    if start_date < first_date:
        start_date = first_date
    if (start_date > last_date) or (end_date < first_date):
        return {'error': 'Interval is out of range'}

    session = Session(engine)

    query = session.query(
                func.min(Measurement.tobs),
                func.round(func.avg(Measurement.tobs), 2),
                func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).\
            filter(Measurement.date <= end_date).first()

    session.close()

    if query == None:
        return {'error': 'Query returned no data'} # Not sure if this is possible. No measurements on this day?

    return {'TMIN': query[0], 'TAVG': query[1], 'TMAX': query[2], 'start': start_date, 'end': end_date}


@app.route("/api/v1.0/<start_date>")
def temp_start(start_date):
    return jsonify(query_db(start_date, last_date))


@app.route("/api/v1.0/<start_date>/<end_date>")
def temp_start_end(start_date, end_date):
    return jsonify(query_db(start_date, end_date))


if __name__ == '__main__':
    app.run(debug=True)
