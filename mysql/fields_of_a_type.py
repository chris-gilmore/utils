#!/usr/bin/env python

import sys
import MySQLdb

def main():
    try:
        field_type = sys.argv[1]
    except Exception as e:
        print >> sys.stderr, "field_type must be one of [tinyint, smallint, mediumint]"
        sys.exit(1)

    server_address="localhost"
    user="root"
    password=""

    db = None
    try:
        db = MySQLdb.connect(host=server_address, user=user, passwd=password, connect_timeout=5)
        c = db.cursor()
        dc = db.cursor(MySQLdb.cursors.DictCursor)

        c.execute("show databases")
        for row in c.fetchall():
            database = row[0]
            if database in ("information_schema", "mysql"):
                continue
            print
            print "Database: %s" % (database)
            db.select_db(database)
            c.execute("show tables")
            for row in c.fetchall():
                table = row[0]
                dc.execute("describe %s" % (table))
                for row in dc.fetchall():
                    field_name = row["Field"]
                    full_field_type = row["Type"]
                    if field_type in full_field_type:
                        print "  %s" % (table)
                        print "    Field:     %s" % (field_name)
                        print "    Type:      %s" % (full_field_type)

    except Exception as e:
        print >> sys.stderr, e
        sys.exit(1)
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    main()
