import json
import urllib.request
import urllib.parse
import requests
import constants
import datetime
import singer
import sqlite3
import sys


def historical_load():
    start_date = datetime.date(2020, 1, 1)
    current_date = datetime.date.today()
    date_list = [(start_date + datetime.timedelta(days=x)).strftime("%Y-%m-%d") for x in
                 range(0, (current_date - start_date).days)]
    payload = {"lat": constants.lat_param, "lng": constants.long_param}

    # async with aiohttp.ClientSession() as session:
    for i, date in enumerate(date_list):
        payload["date"] = date
        payload["id"] = i
        query_string = urllib.parse.urlencode(payload)

        url = constants.api_url + "?" + query_string

        # synchronous requests
        try:
            with urllib.request.urlopen(url) as response:
                response = json.loads(response.read().decode('utf-8'))
                result = response["results"]
                result["id"] = i
                result["date"] = str(date)

                record_data = json.dumps(result)
                schema_data = json.dumps(constants.historical_schema)

                singer.write_schema("atidiv_data", schema_data, "id")
                # print(schema_data)
                singer.write_records("atidiv_data", records=[record_data])
        except Exception as e:
            print(e)

        # asynchronous requests
        # try:
        #     async with session.get(constants.api_url, params=payload) as resp:
        #         response = await resp.json()
        #         # result = response['results']
        #         # result['id'] = i
        #         # result['date'] = str(date)
        #         # record_data = json.dumps(result)
        #         schema_data = json.dumps(constants.historical_schema)
        #         singer.write_schema('atidiv_data', schema_data, "id")
        #         singer.write_records('atidiv_data', records = [record_data]])
        #
        # except Exception as e:
        #     print(e)


def incremental_load():
    todays_date = datetime.date.today()
    params = {"lat": constants.lat_param, "lng": constants.long_param, "date": todays_date}
    try:
        response = requests.get(constants.api_url, params=params)
        data = response.json()
        result = data['results']

        sqliteConnection = sqlite3.connect('main.db')
        cursor = sqliteConnection.cursor()
        sqlite_select_query = """select max(id),max(date) from atidiv_data"""
        cursor.execute(sqlite_select_query)
        record = cursor.fetchall()

        last_id = record[0][0]
        last_date = record[0][1]
        updated_id = int(last_id) + 1

        result["id"] = updated_id
        result["date"] = str(todays_date)

        record_data = json.dumps(result)
        schema_data = json.dumps(constants.historical_schema)

        singer.write_schema("atidiv_data", schema_data, "id")
        singer.write_records("atidiv_data", records=[record_data])

    except Exception as e:
        print(e)


if __name__ == '__main__':
    globals()[sys.argv[1]]()

# asyncio.run(historical_load())
