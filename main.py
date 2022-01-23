import psycopg2
from random import Random


def create_connections():
    """
    """
    return psycopg2.connect(dbname='Fly Booking', user='postgres',
                        password='Peaches1!', host='localhost'), \
           psycopg2.connect(dbname='Hotel Booking', user='postgres',
                     password='Peaches1!', host='localhost'), \
           psycopg2.connect(dbname='Account', user='postgres',
                        password='Peaches1!', host='localhost')


def two_phase_commit(f_conn, h_conn, a_conn):
    """

    """
    trans_id = Random().randint(0, 1000)
    print("Transaction id: ", trans_id)

    f_id = f_conn.xid(trans_id, str(trans_id), "f_id")
    h_id = h_conn.xid(trans_id, str(trans_id), "h_id")
    a_id = a_conn.xid(trans_id, str(trans_id), "a_id")

    f_conn.tpc_begin(f_id)
    h_conn.tpc_begin(h_id)
    a_conn.tpc_begin(a_id)

    f_curr, h_curr, a_curr = f_conn.cursor(), h_conn.cursor(), a_conn.cursor()

    f_curr.execute("INSERT INTO fly_bookings VALUES (111, 'Nik', 'KLM', 'KMP', 'AMS', '2015-05-01')")
    h_curr.execute("INSERT INTO hotel_bookings VALUES (222, 'Nik', 'Hilton', '2015-05-01', '2015-05-01')")
    a_curr.execute("UPDATE account_tbl SET amount = amount - 150 where account_id = 444")

    try:
        f_conn.tpc_prepare()
        h_conn.tpc_prepare()
        a_conn.tpc_prepare()
        print('Preparing Done')
    except psycopg2.DatabaseError as error:
        print(error)
        print("Transaction Rolled Back")
        f_conn.tpc_rollback()
        h_conn.tpc_rollback()
        a_conn.tpc_rollback()
        return
    try:
        f_conn.tpc_commit()
        h_conn.tpc_commit()
        a_conn.tpc_commit()
        print('Commited Successfully')
    except psycopg2.DatabaseError as error:
        print(f'error committing {error}')
        f_conn.tpc_rollback()
        h_conn.tpc_rollback()
        a_conn.tpc_rollback()
        return

    f_conn.close()
    h_conn.close()
    a_conn.close()


if __name__ == "__main__":
    f_conn, h_conn, a_conn = create_connections()

    two_phase_commit(f_conn, h_conn, a_conn)
