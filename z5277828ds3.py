import json
import requests
import sqlite3
from datetime import datetime
from flask import Flask, request
from flask_restplus import abort, Api, Resource
from flask_restplus import reqparse


################### DATABASE STUFF ######################
conn = sqlite3.connect('z5277828.db', check_same_thread=False)
c = conn.cursor()

def setup_db():
    try:
        c.execute('''CREATE TABLE metadata
                    (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    creation_time text,
                    indicator_id text, 
                    indicator_value text)''')

        c.execute('''CREATE TABLE entrydata
                    (indicator_id text,
                    country text,
                    date text,
                    value real)''')
    except Exception as e:
        print("Maybe safe to ignore:", e)

def fetch_indicator_meta_from_db(indicator_id):
    t = (indicator_id,)
    c.execute('SELECT * FROM metadata WHERE indicator_id=?', t)
    record = c.fetchone()
    return record

def insert_new_collection_into_db(indicator_id, indicator_value):
    c.execute("INSERT INTO metadata(creation_time,indicator_id,indicator_value) VALUES ('%s','%s','%s')" \
        % (str(datetime.now().isoformat()),indicator_id,indicator_value))
    conn.commit()

def insert_entries_into_db(entries):
    c.executemany('INSERT INTO entrydata VALUES (?,?,?,?)', entries)
    conn.commit()

def query_entries_for_indicator(indicator_id):
    t = (indicator_id,)
    c.execute('SELECT * FROM entrydata WHERE indicator_id=?', t)
    records = c.fetchall()
    return records

def construct_entries_json_from_records(entry_records):
    return [{"country": r[1], "date": r[2], "value": r[3]} for r in entry_records]

def get_collection_from_db(dbid):
    collection = {}
    t = (dbid,)
    c.execute('SELECT * FROM metadata WHERE id=?', t)
    record = c.fetchone()
    if record is None:
        return {}, False, "collection doesn't exist with id: %d" % dbid
    
    collection['id'] = record[0]
    collection['indicator'] = record[2]
    collection['indicator_value'] = record[3]
    entry_records = query_entries_for_indicator(record[2])
    collection['entries'] = construct_entries_json_from_records(entry_records)
    return collection, True, "All records fetched succesfully!"

def get_ordered_collections(order_param):
    order_query = ""
    if order_param is not None:
        for field in order_param.split(","):
            symbol  = field[0]
            if symbol == "-":
                order = " DESC, "
            else:
                order = " ASC, "
            field = field[1:]
            if field == 'indicator':
                field = 'indicator_id'
            order_query += field + order
    if order_query != "":
        order_query = "ORDER BY " + order_query[:-2]
    c.execute('SELECT * FROM metadata %s' % order_query)
    records = c.fetchall()
    output = [{'uri': "/collections/%d" % r[0], 'id': r[0], 'creation_time': r[1], 'indicator': r[2]} for r in records]
    return output

def delete_collection_from_db(dbid):
    t = (dbid,)
    c.execute('SELECT * FROM metadata WHERE id=?', t)
    record = c.fetchone()
    if record is None:
        return False, "collection doesn't exist with id: %d" % dbid
    c.execute('DELETE FROM metadata WHERE id=?', t)
    t = (record[2],)
    c.execute('DELETE FROM entrydata WHERE indicator_id=?', t)
    return True, "The collection %d was removed from the database!" % dbid

def get_entry_from_db(dbid,year,country):
    t = (dbid,)
    c.execute('SELECT * FROM metadata WHERE id=?', t)
    record = c.fetchone()
    if record is None:
        return None, False, "collection doesn't exist with id: %d" % dbid
    
    t = (record[2],year,country)
    c.execute('SELECT * FROM entrydata WHERE indicator_id=? AND date=? AND country=?', t)
    erecord = c.fetchone()
    if erecord is None:
        return None, False, \
            "Entry doesn't exist in Collection(id=%d, indicator=%s) with year=%s and country=%s"\
                 % (dbid, record[2],year,country) 
    return {
        "id": dbid,
        "indicator": record[2],
        "country": country,
        "year": year,
        "value": erecord[3],
    }, True, "Found the entry!"

def get_entries_sorted_by_values(dbid, year, n, order_string):
    t = (dbid,)
    c.execute('SELECT * FROM metadata WHERE id=?', t)
    record = c.fetchone()
    if record is None:
        return None, False, "collection doesn't exist with id: %d" % dbid
    
    t = (record[2],year)
    if n is None:
        limit_string = ""
    else:
        limit_string = "LIMIT %d" % n
    
    c.execute('SELECT * FROM entrydata WHERE indicator_id=? AND date=? ORDER BY value %s %s'\
         % (order_string, limit_string), t)
    erecords = c.fetchall()
    
    if order_string == 'ASC':
        m_string  = 'bottom'
        erecords = erecords[::-1] # Reverse the entries because highest should be on top
    else:
        m_string = 'top'

    entries = [{"country": r[1], "value": r[3]} for r in erecords]
    return {
        "indicator": record[2],
        "indicator_value": record[3],
        "entries": entries,
    }, True, "Fetched %s %d entries from collection %s" % (m_string, len(entries), record[2])

def close_db():
    c.execute('''DROP TABLE metadata''')
    c.execute('''DROP TABLE entrydata''')
    conn.close()



################# API STUFF #####################

def fetch_json(url):
    try:
        r = requests.get(url)
        if r.status_code != 200:
            return None, False, "GET on %s returned response code: %d" % (url, r.status_code)
        return r.json(), True, "Fetched successfully"
    except Exception as e:
        return None, False, str(e)

def check_json(j):
    if len(j) == 1:
        return False, "Data seems to be unavailable: Please check indicator_id!"
    return True, "JSON seems to be correct!"

def get_entries(json_data):
    try:
        entries = []
        for item in json_data:
            if item.get('value', None) == None:
                continue
            entry = (item['indicator']['id'], item['country']['value'],
                item['date'], item['value'])
            entries.append(entry)
        return (get_metadata(json_data),entries), True, ""
    except Exception as e:
        return (), False, str(e)

def get_metadata(json_data):
    return json_data[0]['indicator']

def fetch_data_for_indicator(indicator_id, per_page_count):
    URL = "http://api.worldbank.org/v2/countries/all/indicators/%s?date=2012:2017&format=json&per_page=%d" \
        % (indicator_id, per_page_count)
    
    j, success, message = fetch_json(URL)
    if not success:
        return [], False, message

    passed, message = check_json(j)
    if not passed:
        return [], False, message
    
    page_data = j[0]

    # Data not fully fetched! More than 1 page available.
    if page_data['pages'] != 1:
        total_entries = page_data['total']
        return fetch_data_for_indicator(indicator_id, total_entries)

    return get_entries(j[1])


############ FLASK STUFF ###################
app = Flask(__name__)
api = Api(app)

add_collection_args =  reqparse.RequestParser()
add_collection_args.add_argument("indicator_id", type=str, required=True)

get_collection_args = reqparse.RequestParser()
get_collection_args.add_argument("order_by", type=str, required=False)

@api.route('/collections')
class AddCollections(Resource):

    @api.expect(add_collection_args, validate=True)
    def post(self):
        indicator_id = add_collection_args.parse_args().get('indicator_id', None)
        if indicator_id == None:
            abort(400, 'indicator_id not present in the request args', success='False')
        
        imeta = fetch_indicator_meta_from_db(indicator_id)
        if imeta is not None:
            abort(400, 'indicator_id already imported in collections', success='False')

        meta_and_entries, success, message = fetch_data_for_indicator(indicator_id, 1000)
        if not success:
            abort(400, message, success=False)

        meta, entries = meta_and_entries
        insert_new_collection_into_db(meta['id'], meta['value'])
        imeta = fetch_indicator_meta_from_db(indicator_id)
        insert_entries_into_db(entries)
        return {
            'id': imeta[0],
            'uri': "/collections/%d" % imeta[0],
            'creation_time': imeta[1],
            'indicator_id': imeta[2],
            'success': True,
            'message': "Collection added successfully!"
        }, 201

    @api.expect(get_collection_args, validate=True)
    def get(self):
        order_param = get_collection_args.parse_args().get('order_by', None)
        return get_ordered_collections(order_param), 200

@api.route('/collections/<int:id>')
class ViewDeleteCollections(Resource):
    def get(self, id):
        collection, success, message = get_collection_from_db(id)
        if not success:
            abort(400, message, success=False)
        return collection, 200

    def delete(self, id):
        success, message = delete_collection_from_db(id)
        if not success:
            abort(400, message, success=False)
        return {
            "id": id,
            "message": message,
            "success": success,
        }, 200

@api.route('/collections/<int:id>/<string:year>/<string:country>')
class SpecificEntry(Resource):
    def get(self, id, year, country):
        data, success, message = get_entry_from_db(id,year,country)
        if not success:
            abort(400, message, success=False)
        return data, 200

top_n_args =  reqparse.RequestParser()
top_n_args.add_argument("q", type=str, required=False)
@api.route('/collections/<int:id>/<string:year>')
class TopEntries(Resource):
    @api.expect(top_n_args)
    def get(self, id, year):
        n = top_n_args.parse_args().get('q', None)
        if n is None:
            results, success, message = get_entries_sorted_by_values(id,year,n,'ASC')
        else:
            if '-' in n:
                results, success, message = get_entries_sorted_by_values(id,year,int(n[1:]),'ASC')
            else:
                if '+' in n:
                    n = n[1:]
                results, success, message = get_entries_sorted_by_values(id,year,int(n),'DESC')
        if not success:
            abort(400, message, success=False)
        
        return results, 200

if __name__ == '__main__':
    setup_db()
    app.run(debug=True)