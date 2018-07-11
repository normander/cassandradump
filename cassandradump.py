import argparse
import sys
import itertools
import codecs
from ssl import PROTOCOL_TLSv1
import six
import re

try:
    import cassandra
    import cassandra.concurrent
except ImportError:
    sys.exit('Python Cassandra driver not installed. You might try \"pip install cassandra-driver\".')

from cassandra.auth import PlainTextAuthProvider #For protocol_version 2
from cassandra.cluster import Cluster

TIMEOUT = 120.0
FETCH_SIZE = 100
DOT_EVERY = 1000
CONCURRENT_BATCH_SIZE = 1000

args = None

def cql_type(val):
    try:
        return val.data_type.typename
    except AttributeError:
        return val.cql_type

def to_utf8(string):
    return codecs.decode(string, 'utf-8')

def log_quiet(msg):
    if not args.quiet:
        sys.stdout.write(msg)
        sys.stdout.flush()


def table_to_cqlfile(session, keyspace, tablename, flt, tableval, filep, limit=0):
    if flt is None:
        query = 'SELECT * FROM "' + keyspace + '"."' + tablename + '"'
    else:
        query = 'SELECT * FROM ' + flt

    if limit > 0:
        query = query + " LIMIT " + str(limit)

    rows = session.execute(query)

    cnt = 0

    def cql_encode_object(val):
        """
        Default encoder for all objects that do not have a specific encoder function
        registered. This function simply calls :meth:`str()` on the object.
        """
        if isinstance(val, object):
            #if our object is a UDT, give cassandra what it wants (using JSON to process), and regex to clean
            if type(val).__module__.startswith("cassandra"):
                return '{%s}' % ', '.join('%s: %s' % (
                    k,
                    session.encoder.mapping.get(type(v), cql_encode_object)(v)
                ) for k, v in six.iteritems(val.__dict__))
        return str(val)

    def cql_encode_map_collection(val):
        """
        Converts a dict into a string of the form ``{key1: val1, key2: val2, ...}``.
        This is suitable for ``map`` type columns.
        """
        return '{%s}' % ', '.join('%s: %s' % (
            session.encoder.mapping.get(type(k), cql_encode_object)(k),
            session.encoder.mapping.get(type(v), cql_encode_object)(v)
        ) for k, v in six.iteritems(val))

    def cql_encode_set_collection(val):
        """
        Converts a sequence to a string of the form ``{item1, item2, ...}``.  This
        is suitable for ``set`` type columns.
        """
        return '{%s}' % ', '.join(session.encoder.mapping.get(type(v), cql_encode_object)(v) for v in val)

    def cql_encode_list_collection(val):
        """
        Converts a sequence to a string of the form ``[item1, item2, ...]``.  This
        is suitable for ``list`` type columns.
        """
        return '[%s]' % ', '.join(session.encoder.mapping.get(type(v), cql_encode_object)(v) for v in val)


    def make_non_null_value_encoder(typename):
        if typename == 'blob':
            return session.encoder.cql_encode_bytes
        elif typename.startswith('map'):
            return cql_encode_map_collection
        elif typename.startswith('set'):
            return cql_encode_set_collection
        elif typename.startswith('list'):
            return cql_encode_list_collection
        else:
            return  session.encoder.cql_encode_all_types

    def make_value_encoder(typename):
        e = make_non_null_value_encoder(typename)
        return lambda v: session.encoder.cql_encode_all_types(v) if v is None else e(v)

    def make_value_encoders(tableval):
        return dict((to_utf8(k), make_value_encoder(cql_type(v))) for k, v in tableval.columns.iteritems())

    def make_row_encoder():
        partitions = dict(
            (has_counter, list(to_utf8(k) for k, v in columns))
            for has_counter, columns in itertools.groupby(tableval.columns.iteritems(), lambda (k, v): cql_type(v) == 'counter')
        )

        keyspace_utf8 = to_utf8(keyspace)
        tablename_utf8 = to_utf8(tablename)

        counters = partitions.get(True, [])
        non_counters = partitions.get(False, [])
        columns = counters + non_counters

        if len(counters) > 0:
            def row_encoder(values):
                set_clause = ", ".join('%s = %s + %s' % (c, c, values[c]) for c in counters if values[c] != 'NULL')
                where_clause = " AND ".join('%s = %s' % (c, values[c]) for c in non_counters)
                return 'UPDATE "%(keyspace)s"."%(tablename)s" SET %(set_clause)s WHERE %(where_clause)s' % dict(
                    keyspace=keyspace_utf8,
                    tablename=tablename_utf8,
                    where_clause=where_clause,
                    set_clause=set_clause,
                )
        else:
            columns = list(counters + non_counters)
            def row_encoder(values):
                return 'INSERT INTO "%(keyspace)s"."%(tablename)s" (%(columns)s) VALUES (%(values)s)' % dict(
                    keyspace=keyspace_utf8,
                    tablename=tablename_utf8,
                    columns=', '.join('"{}"'.format(c) for c in columns if values[c] != "NULL"),
                    values=', '.join(values[c] for c in columns if values[c] != "NULL"),
                )
        return row_encoder

    value_encoders = make_value_encoders(tableval)
    row_encoder = make_row_encoder()

    for row in rows:
        values = dict((to_utf8(k), to_utf8(value_encoders[k](v))) for k, v in row.iteritems())
        filep.write("%s;\n" % row_encoder(values))

        cnt += 1

        if (cnt % DOT_EVERY) == 0:
            log_quiet('.')

    if cnt > DOT_EVERY:
        log_quiet('\n')


def can_execute_concurrently(statement):
    if args.sync:
        return False

    if statement.upper().startswith('INSERT') or statement.upper().startswith('UPDATE'):
        return True
    else:
        return False


def import_data(session):
    fp = codecs.open(args.import_file, 'r', encoding='utf-8')

    cnt = 0

    statement = ''
    concurrent_statements = []

    for line in fp:
        statement += line
        if statement.endswith(";\n"):
            if can_execute_concurrently(statement):
                concurrent_statements.append((statement, None))

                if len(concurrent_statements) >= CONCURRENT_BATCH_SIZE:
                    cassandra.concurrent.execute_concurrent(session, concurrent_statements)
                    concurrent_statements = []
            else:
                if len(concurrent_statements) > 0:
                    cassandra.concurrent.execute_concurrent(session, concurrent_statements)
                    concurrent_statements = []

                session.execute(statement)

            statement = ''

            cnt += 1
            if (cnt % DOT_EVERY) == 0:
                log_quiet('.')

    if len(concurrent_statements) > 0:
        cassandra.concurrent.execute_concurrent(session, concurrent_statements)

    if statement != '':
        session.execute(statement)

    if cnt > DOT_EVERY:
        log_quiet('\n')

    fp.close()


def get_keyspace_or_fail(session, keyname):
    keyspace = session.cluster.metadata.keyspaces.get(keyname)

    if not keyspace:
        sys.stderr.write('Can\'t find keyspace "' + keyname + '"\n')
        sys.exit(1)

    return keyspace


def get_column_family_or_fail(keyspace, tablename):
    tableval = keyspace.tables.get(tablename)

    if not tableval:
        sys.stderr.write('Can\'t find table "' + tablename + '"\n')
        sys.exit(1)

    return tableval


def export_data(session):
    selection_options = 0

    if args.keyspace is not None:
        selection_options += 1

    if args.cf is not None:
        selection_options += 1

    if args.filter is not None:
        selection_options += 1

    if selection_options > 1:
        sys.stderr.write('--cf, --keyspace and --filter can\'t be combined\n')
        sys.exit(1)

    f = codecs.open(args.export_file, 'w', encoding='utf-8')

    keyspaces = None
    exclude_list = []

    if selection_options == 0:
        log_quiet('Exporting all keyspaces\n')
        keyspaces = []
        for keyspace in session.cluster.metadata.keyspaces.keys():
            if keyspace not in ('system', 'system_traces'):
                keyspaces.append(keyspace)

    if args.limit is not None:
        limit = int(args.limit)
    else:
        limit = 0

    if args.keyspace is not None:
        keyspaces = args.keyspace
        if args.exclude_cf is not None:
            exclude_list.extend(args.exclude_cf)

    if keyspaces is not None:
        for keyname in keyspaces:
            keyspace = get_keyspace_or_fail(session, keyname)

            if not args.no_create:
                log_quiet('Exporting schema for keyspace ' + keyname + '\n')
                f.write('DROP KEYSPACE IF EXISTS "' + keyname + '";\n')
                f.write(keyspace.export_as_string() + '\n')

            for tablename, tableval in keyspace.tables.iteritems():
                if tablename in exclude_list:
                    log_quiet('Skipping data export for column family ' + keyname + '.' + tablename + '\n')
                    continue
                elif tableval.is_cql_compatible:
                    if not args.no_insert:
                        log_quiet('Exporting data for column family ' + keyname + '.' + tablename + '\n')
                        table_to_cqlfile(session, keyname, tablename, None, tableval, f, limit)

    if args.cf is not None:
        for cf in args.cf:
            if '.' not in cf:
                sys.stderr.write('Invalid keyspace.column_family input\n')
                sys.exit(1)

            keyname = cf.split('.')[0]
            tablename = cf.split('.')[1]

            keyspace = get_keyspace_or_fail(session, keyname)
            tableval = get_column_family_or_fail(keyspace, tablename)

            if tableval.is_cql_compatible:
                if not args.no_create:
                    log_quiet('Exporting schema for column family ' + keyname + '.' + tablename + '\n')
                    f.write('DROP TABLE IF EXISTS "' + keyname + '"."' + tablename + '";\n')
                    f.write(tableval.export_as_string() + ';\n')

                if not args.no_insert:
                    log_quiet('Exporting data for column family ' + keyname + '.' + tablename + '\n')
                    table_to_cqlfile(session, keyname, tablename, None, tableval, f, limit)

    if args.filter is not None:
        for flt in args.filter:
            stripped = flt.strip()
            cf = stripped.split(' ')[0]

            if '.' not in cf:
                sys.stderr.write('Invalid input\n')
                sys.exit(1)

            keyname = cf.split('.')[0]
            tablename = cf.split('.')[1]


            keyspace = get_keyspace_or_fail(session, keyname)
            tableval = get_column_family_or_fail(keyspace, tablename)

            if not tableval:
                sys.stderr.write('Can\'t find table "' + tablename + '"\n')
                sys.exit(1)

            if not args.no_insert:
                log_quiet('Exporting data for filter "' + stripped + '"\n')
                table_to_cqlfile(session, keyname, tablename, stripped, tableval, f, limit)

    f.close()

def get_credentials():
    return {'username': args.username, 'password': args.password}

def setup_cluster():
    if args.host is None:
        nodes = ['localhost']
    else:
        nodes = [args.host]

    if args.port is None:
        port = 9042
    else:
        port = int(args.port)

    if args.connect_timeout is None:
        connect_timeout = 5
    else:
        connect_timeout = int(args.connect_timeout)

    if args.ssl is not None and args.certfile is not None:
      ssl_opts = { 'ca_certs': args.certfile,
                   'ssl_version': PROTOCOL_TLSv1,
                   'keyfile': args.userkey,
                   'certfile': args.usercert }
    else:
      ssl_opts = {}

    cluster = None

    if args.protocol_version is not None:
        auth = None

        if args.username is not None and args.password is not None:
            if args.protocol_version == 1:
                auth = get_credentials
            elif args.protocol_version > 1:
                auth = PlainTextAuthProvider(username=args.username, password=args.password)

        cluster = Cluster(control_connection_timeout=connect_timeout, connect_timeout=connect_timeout, contact_points=nodes, port=port, protocol_version=args.protocol_version, auth_provider=auth, load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy(nodes), ssl_options=ssl_opts)
    else:
        cluster = Cluster(control_connection_timeout=connect_timeout, connect_timeout=connect_timeout, contact_points=nodes, port=port, load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy(nodes), ssl_options=ssl_opts)

    session = cluster.connect()

    session.default_timeout = TIMEOUT
    session.default_fetch_size = FETCH_SIZE
    session.row_factory = cassandra.query.ordered_dict_factory
    return session


def cleanup_cluster(session):
    session.cluster.shutdown()
    session.shutdown()

def rename_keyspace():
    old_keyspace = str(args.keyspace[0])
    new_keyspace = args.new_keyspace_name

    with open(str(args.export_file), 'r') as f:
        newlines = []
        for line in f.readlines():
            cluster_name = re.search(r'{\'class\': \'NetworkTopologyStrategy\', \'([^\']*)\'', line)
            if cluster_name is not None and args.new_cluster_name is not None:
                newlines.append(
                    line.replace(str(cluster_name.group(1)), str(args.new_cluster_name)).replace(str(old_keyspace),
                                                                                                 str(new_keyspace)))
            else:
                newlines.append(line.replace(str(old_keyspace), str(new_keyspace)))
    with open(str(args.export_file), 'w') as f:
        for line in newlines:
            f.write(line)

    print "Name of keyspace was changed, ", old_keyspace, "->", new_keyspace

def main():
    global args

    parser = argparse.ArgumentParser(description='A data exporting tool for Cassandra inspired from mysqldump, with some added slice and dice capabilities.')
    parser.add_argument('--connect-timeout', help='set timeout for connecting to the cluster (in seconds)', type=int)
    parser.add_argument('--cf', help='export a column family. The name must include the keyspace, e.g. "system.schema_columns". Can be specified multiple times', action='append')
    parser.add_argument('--export-file', help='export data to the specified file')
    parser.add_argument('--filter', help='export a slice of a column family according to a CQL filter. This takes essentially a typical SELECT query stripped of the initial "SELECT ... FROM" part (e.g. "system.schema_columns where keyspace_name =\'OpsCenter\'", and exports only that data. Can be specified multiple times', action='append')
    parser.add_argument('--host', help='the address of a Cassandra node in the cluster (localhost if omitted)')
    parser.add_argument('--port', help='the port of a Cassandra node in the cluster (9042 if omitted)')
    parser.add_argument('--import-file', help='import data from the specified file')
    parser.add_argument('--keyspace', help='export a keyspace along with all its column families. Can be specified multiple times', action='append')
    parser.add_argument('--exclude-cf', help='when using --keyspace, specify column family to exclude.  Can be specified multiple times', action='append')
    parser.add_argument('--no-create', help='don\'t generate create (and drop) statements', action='store_true')
    parser.add_argument('--no-insert', help='don\'t generate insert statements', action='store_true')
    parser.add_argument('--password', help='set password for authentication (only if protocol-version is set)')
    parser.add_argument('--protocol-version', help='set protocol version (required for authentication)', type=int)
    parser.add_argument('--quiet', help='quiet progress logging', action='store_true')
    parser.add_argument('--sync', help='import data in synchronous mode (default asynchronous)', action='store_true')
    parser.add_argument('--username', help='set username for auth (only if protocol-version is set)')
    parser.add_argument('--limit', help='set number of rows return limit')
    parser.add_argument('--ssl', help='enable ssl connection to Cassandra cluster.  Must also set --certfile.', action='store_true')
    parser.add_argument('--certfile', help='ca cert file for SSL.  Assumes --ssl.')
    parser.add_argument('--userkey', help='user key file for client authentication.  Assumes --ssl.')
    parser.add_argument('--usercert', help='user cert file for client authentication.  Assumes --ssl.')
    parser.add_argument('--new-keyspace-name', help='set new name of keyspace - option for export keyspace to another cluster')
    parser.add_argument('--new-cluster-name', help='new name of cluster - option for export keyspace to another cluster')

    args = parser.parse_args()

    if args.import_file is None and args.export_file is None:
        sys.stderr.write('--import-file or --export-file must be specified\n')
        sys.exit(1)

    if (args.userkey is not None or args.usercert is not None) and (args.userkey is None or args.usercert is None):
        sys.stderr.write('--userkey and --usercert must both be provided\n')
        sys.exit(1)

    if args.import_file is not None and args.export_file is not None:
        sys.stderr.write('--import-file and --export-file can\'t be specified at the same time\n')
        sys.exit(1)

    if args.ssl is True and args.certfile is None:
        sys.stderr.write('--certfile must also be specified when using --ssl\n')
        sys.exit(1)

    session = setup_cluster()

    if args.import_file:
        import_data(session)
    elif args.export_file:
        export_data(session)

    if args.new_keyspace_name is not None and args.export_file is not None:
        if len(args.keyspace) < 2:
            rename_keyspace()
        else:
            sys.stderr.write('New name for keyspace is only available when you exporting single keyspace!')
            sys.exit(1)

    cleanup_cluster(session)


if __name__ == '__main__':
    main()
