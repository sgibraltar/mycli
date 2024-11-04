import threading
import os
import json
import sqlite3
import hashlib
from .packages.special.main import COMMANDS
from collections import OrderedDict

from .sqlcompleter import SQLCompleter
from .sqlexecute import SQLExecute, ServerSpecies


def get_sql_executor(sqlexecute, db=None):
    e = sqlexecute
    return SQLExecute(
        db or e.dbname,
        e.user,
        e.password,
        e.host,
        e.port,
        e.socket,
        e.charset,
        e.local_infile,
        e.ssl,
        e.ssh_user,
        e.ssh_host,
        e.ssh_port,
        e.ssh_password,
        e.ssh_key_filename,
    )


def refresh_db(completer, executor, db):
    if executor is None or db is None:
        return

    refresh_databases(completer, executor)

    if len(completer.populate_schema_objects(db, "tables")) == 0:
        refresh_schemata(completer, executor)
        completer.set_dbname(executor.dbname)
        refresh_tables(completer, executor)

        completer.populate_schema_objects(db, "tables")


def get_executor_hash(executor):
    return hashlib.md5(f"{executor.host}{executor.port}{executor.user}".encode()).hexdigest()


def cache_dbmetadata(completer, executor):
    """Cache the database metadata in a SQLite database.

    The cache is stored in a table called dbmetadata with a single row.
    The row contains the pickled dbmetadata dictionary.

    """
    if completer.dbmetadata is None or executor is None:
        return

    executor_hash = get_executor_hash(executor)

    with sqlite3.connect(os.path.expanduser(completer.completion_cache_file)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dbmetadata ("
            " connection_hash, metadata, "
            " created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            " PRIMARY KEY(connection_hash) "
            ")")

        conn.execute(
            "INSERT INTO dbmetadata (connection_hash, metadata) VALUES (?, ?) "
            "ON CONFLICT(connection_hash) DO UPDATE SET "
            "metadata = excluded.metadata, "
            "created_at = CURRENT_TIMESTAMP",
            (executor_hash, json.dumps(completer.dbmetadata))
        )
        conn.commit()


def load_cached_dbmetadata(completer, executor, num_days=1):
    """Load the database metadata from the cache.

    The cache is stored in a table called dbmetadata with a single row.
    The row contains the pickled dbmetadata dictionary.

    """
    if executor is None:
        return

    if num_days is None or num_days < 0:
        num_days = 1

    executor_hash = get_executor_hash(executor)

    try:
        with sqlite3.connect(os.path.expanduser(completer.completion_cache_file)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT metadata FROM dbmetadata "
                "WHERE connection_hash = ? "
                f"AND created_at >= datetime('now', '-{num_days} day') "
                "LIMIT 1",
                (executor_hash,)
            )
            row = cursor.fetchone()

        if row is not None:
            completer.merge_completions(json.loads(row[0]))
    except sqlite3.OperationalError:
        pass


class CompletionRefresher(object):
    refreshers = OrderedDict()
    scoped_db_name = None
    prev_dbmetadata = None

    def __init__(self):
        self._completer_thread = None
        self._restart_refresh = threading.Event()

    def refresh(self, executor, callbacks, completer_options=None, scoped_db_name=None, prev_dbmetadata=None):
        """Creates a SQLCompleter object and populates it with the relevant
        completion suggestions in a background thread.

        executor - SQLExecute object, used to extract the credentials to connect
                   to the database.
        callbacks - A function or a list of functions to call after the thread
                    has completed the refresh. The newly created completion
                    object will be passed in as an argument to each callback.
        completer_options - dict of options to pass to SQLCompleter.
        scoped_db_name - The database name to scope the completions to.
        prev_dbmetadata - The previous dbmetadata to merge with the new one.

        """
        self.scoped_db_name = scoped_db_name
        self.prev_dbmetadata = prev_dbmetadata

        if completer_options is None:
            completer_options = {}

        if self.is_refreshing():
            self._restart_refresh.set()
            return [(None, None, None, "Auto-completion refresh restarted.")]
        else:
            self._completer_thread = threading.Thread(
                target=self._bg_refresh,
                args=(executor, callbacks, completer_options),
                name="completion_refresh",
            )
            self._completer_thread.daemon = True
            self._completer_thread.start()
            return [
                (None, None, None, "Auto-completion refresh started in the background.")
            ]

    def is_refreshing(self):
        return self._completer_thread and self._completer_thread.is_alive()

    def _bg_refresh(self, sqlexecute, callbacks, completer_options):
        completer = SQLCompleter(**completer_options)

        # Create a new pgexecute method to populate the completions.
        executor = get_sql_executor(sqlexecute, self.scoped_db_name)

        # If callbacks is a single function then push it into a list.
        if callable(callbacks):
            callbacks = [callbacks]

        if completer.completion_cache_file and self.prev_dbmetadata is None:
            load_cached_dbmetadata(completer, executor)

        # if self.prev_dbmetadata and isinstance(self.prev_dbmetadata, str):
        #     completer.merge_completions(json.loads(self.prev_dbmetadata))

        if self.scoped_db_name and self.prev_dbmetadata and isinstance(self.prev_dbmetadata, dict):
            refresh_db(completer, executor, self.scoped_db_name)
            completer.merge_completions(self.prev_dbmetadata)
            cache_dbmetadata(completer, executor)

            if self._restart_refresh.is_set():
                self._restart_refresh.clear()
        else:
            while 1:
                for refresher in self.refreshers.values():
                    refresher(completer, executor)
                    if self._restart_refresh.is_set():
                        self._restart_refresh.clear()
                        break
                else:
                    # Break out of while loop if the for loop finishes natually
                    # without hitting the break statement.
                    break

                # Start over the refresh from the beginning if the for loop hit the
                # break statement.
                continue


        for callback in callbacks:
            callback(completer)


def refresher(name, refreshers=CompletionRefresher.refreshers):
    """Decorator to add the decorated function to the dictionary of
    refreshers. Any function decorated with a @refresher will be executed as
    part of the completion refresh routine."""

    def wrapper(wrapped):
        refreshers[name] = wrapped
        return wrapped

    return wrapper


@refresher("databases")
def refresh_databases(completer, executor):
    dbs = executor.databases()
    completer.extend_database_names(dbs)


@refresher("schemata")
def refresh_schemata(completer, executor):
    # schemata - In MySQL Schema is the same as database. But for mycli
    # schemata will be the name of the current database.
    completer.extend_schemata(executor.dbname)
    completer.set_dbname(executor.dbname)


@refresher("tables")
def refresh_tables(completer, executor):
    completer.extend_relations(executor.tables(), kind="tables")
    completer.extend_columns(executor.table_columns(), kind="tables")


@refresher("users")
def refresh_users(completer, executor):
    completer.extend_users(executor.users())


# @refresher('views')
# def refresh_views(completer, executor):
#     completer.extend_relations(executor.views(), kind='views')
#     completer.extend_columns(executor.view_columns(), kind='views')


@refresher("functions")
def refresh_functions(completer, executor):
    completer.extend_functions(executor.functions())
    if executor.server_info.species == ServerSpecies.TiDB:
        completer.extend_functions(completer.tidb_functions, builtin=True)


@refresher("special_commands")
def refresh_special(completer, executor):
    completer.extend_special_commands(COMMANDS.keys())


@refresher("show_commands")
def refresh_show_commands(completer, executor):
    completer.extend_show_items(executor.show_candidates())


@refresher("keywords")
def refresh_keywords(completer, executor):
    if executor.server_info.species == ServerSpecies.TiDB:
        completer.extend_keywords(completer.tidb_keywords, replace=True)
