#!/usr/bin/env python3

import sys
from operator import itemgetter
import ujson
import psycopg2
from psycopg2.extras import execute_values
import config


def calculate_top_artists():

    artist_dict = {}
    count = 0
    with psycopg2.connect(config.DB_CONNECT) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute("""SELECT data->'track_metadata'->'additional_info'->>'artist_msid' AS artist_msid 
                              FROM listen""")
            while True:
                row = curs.fetchone()
                if not row:
                    break

                msid = row['artist_msid']
                try:
                    artist_dict[msid] += 1
                except KeyError:
                    artist_dict[msid] = 1

                count += 1
                if count % 1000000 == 0:
                    print("%d rows processed" % count)

    print("collect output")
    output = []
    for msid in artist_dict.keys():
        output.append((msid, artist_dict[msid]))

    print("sort output")
    output = sorted(output, key=itemgetter(1), reverse=True)

    print("write output")
    with open("popular_artist_msids.json", "w") as f:
        f.write(ujson.dumps(output))



def usage(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


if __name__ == "__main__":
    calculate_top_artists()
    sys.exit(0)
