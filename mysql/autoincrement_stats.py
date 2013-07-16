#!/usr/bin/env python

import sys
import MySQLdb

def main():
    min_percent = float(sys.argv[1]) if len(sys.argv) > 1 else 0

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
                    if row["Extra"] == "auto_increment":
                        field_name = row["Field"]
                        full_field_type = row["Type"]
                        unsigned = ("unsigned" in full_field_type)
                        for integer_type in ("tinyint", "smallint", "mediumint", "bigint", "int"):
                            if integer_type in full_field_type:
                                field_type = integer_type
                                break
                        if field_type == 'tinyint':
                            max_value = 255 if unsigned else 127
                        elif field_type == 'smallint':
                            max_value = 65535 if unsigned else 32767
                        elif field_type == 'mediumint':
                            max_value = 16777215 if unsigned else 8388607
                        elif field_type == 'int':
                            max_value = 4294967295 if unsigned else 2147483647
                        elif field_type == 'bigint':
                            max_value = 18446744073709551615 if unsigned else 9223372036854775807

                        dc.execute("show table status like %s", (table,))
                        row = dc.fetchone()
                        next_id = row["Auto_increment"]

                        percent = 100.0 * next_id / max_value
                        if percent >= min_percent:
                            print "  %s" % (table)
                            print "    Field:     %s" % (field_name)
                            print "    Type:      %s" % (full_field_type)
                            print "    max_value: %s" % (max_value)
                            print "    next_id:   %s" % (next_id)
                            print "    percent:   %s" % (percent)
                        break

    except Exception as e:
        print >> sys.stderr, e
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    main()
