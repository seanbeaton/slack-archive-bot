import argparse
import datetime
import logging
import os
import sqlite3
import time
import traceback
import json
import re
import hashlib

from slackclient import SlackClient
from websocket import WebSocketConnectionClosedException

default_messages_before = 5
default_messages_after = 5
default_results_limit = 10


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--database-path', default='slack.sqlite', help=(
                    'path to the SQLite database. (default = ./slack.sqlite)'))
parser.add_argument('-l', '--log-level', default='debug', help=(
                    'CRITICAL, ERROR, WARNING, INFO or DEBUG (default = DEBUG)'))
args = parser.parse_args()

log_level = args.log_level.upper()
assert log_level in ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger(__name__)

database_path = args.database_path

# Connects to the previously created SQL database
conn = sqlite3.connect(database_path)
cursor = conn.cursor()
cursor.execute('create table if not exists messages (message text, user text, channel text, timestamp text, hash text, UNIQUE(channel, timestamp) ON CONFLICT REPLACE)')
cursor.execute('create table if not exists users (name text, id text, avatar text, UNIQUE(id) ON CONFLICT REPLACE)')
cursor.execute('create table if not exists channels (name text, id text, UNIQUE(id) ON CONFLICT REPLACE)')
cursor.execute('create table if not exists searches (query text, user text, results text, timestamp text, UNIQUE(user, timestamp) ON CONFLICT REPLACE)')

# This token is given when the bot is started in terminal
slack_token = os.environ["SLACK_API_TOKEN"]

# Makes bot user active on Slack
# NOTE: terminal must be running for the bot to continue
sc = SlackClient(slack_token)

# Double naming for better search functionality
# Keys are both the name and unique ID where needed
ENV = {
    'user_id': {},
    'id_user': {},
    'channel_id': {},
    'id_channel': {},
    'channel_info': {}
}

def update_database():
    try:
        cursor.execute('ALTER TABLE messages ADD COLUMN hash text;')
        conn.commit()
        # column didn't exist, populate column
    except:
        # column already exists
        pass
    finally:
        logger.debug('Creating hashes of messages')
        cursor.execute('SELECT user, timestamp FROM messages WHERE hash is NULL')
        for row in cursor.fetchall():
            cursor.execute('UPDATE messages SET hash = (?) WHERE user = (?) and timestamp = (?)', (get_event_hash({'user': row[0], 'ts': row[1]}), row[0], row[1]))
        conn.commit()
        logger.debug("Done creating hashes of messages")


# Uses slack API to get most recent user list
# Necessary for User ID correlation
def update_users():
    logger.info('Updating users')
    info = sc.api_call('users.list')
    ENV['user_id'] = dict([(m['name'], m['id']) for m in info['members']])
    ENV['id_user'] = dict([(m['id'], m['name']) for m in info['members']])

    args = []
    for m in info['members']:
        args.append((
            m['name'],
            m['id'],
            m['profile'].get('image_72', 'https://secure.gravatar.com/avatar/c3a07fba0c4787b0ef1d417838eae9c5.jpg?s=32&d=https%3A%2F%2Ffst.slack-edge.com%2F66f9%2Fimg%2Favatars%2Fava_0024-32.png')
        ))
    cursor.executemany("INSERT INTO users(name, id, avatar) VALUES(?,?,?)", args)
    conn.commit()

def get_user_name(uid):
    if uid not in ENV['id_user']:
        update_users()
    return ENV['id_user'].get(uid, None)

def get_user_id(name):
    if name not in ENV['user_id']:
        update_users()
    return ENV['user_id'].get(name, None)


def parse_time_to_minutes(string):
    """
    Parses time from the user to a number of minutes. Potential inputs include:
    1minute
    2hours
    15days
    3weeks
    :param string:
    :return:
    """
    multipliers = {
        'w': 7 * 24 * 60,
        'week': 7 * 24 * 60,
        'weeks': 7 * 24 * 60,
        'd': 24 * 60,
        'day': 24 * 60,
        'days': 24 * 60,
        'h': 60,
        'hour': 60,
        'hours': 60,
        'm': 1,
        'minute': 1,
        'minutes': 1,
    }

    res = re.search('^(?P<quantity>[\d]+)(?P<unit>[a-zA-Z]+)$', string)
    if not res or not res['quantity'] or not res['unit']:
        raise ValueError('Unable to parse amount of time `%s`' % string)
    if res['unit'] not in multipliers:
        raise ValueError('`%s` is not a valid unit of time. Use /help to see valid options.' % res['unit'])
    return multipliers[res['unit']] * float(res['quantity'])

def update_channels():
    logger.info("Updating channels")
    info = sc.api_call('channels.list')['channels'] + sc.api_call('groups.list')['groups']
    ENV['channel_id'] = dict([(m['name'], m['id']) for m in info])
    ENV['id_channel'] = dict([(m['id'], m['name']) for m in info])

    args = []
    for m in info:
        ENV['channel_info'][m['id']] = {
            'is_private': ('is_group' in m) or m['is_private'],
            'members': m['members']
        }

        args.append((
            m['name'],
            m['id']
        ))

    cursor.executemany("INSERT INTO channels(name, id) VALUES(?,?)", args)
    conn.commit()

def get_channel_name(uid):
    if uid not in ENV['id_channel']:
        update_channels()
    return ENV['id_channel'].get(uid, None)

def get_channel_id(name):
    if name not in ENV['channel_id']:
        update_channels()
    return ENV['channel_id'].get(name, None)


def get_event_hash(event):
    user = event['user']
    ts = event['ts'] if 'ts' in event else event['timestamp']
    h = hashlib.sha256()
    h.update(str(user + ts).encode('utf-8'))
    return h.hexdigest()


def send_message(message, channel):
    sc.api_call(
      "chat.postMessage",
      channel=channel,
      text=message
    )

def convert_timestamp(ts):
    return datetime.datetime.fromtimestamp(
        int(ts.split('.')[0])
    ).strftime('%Y-%m-%d %H:%M:%S')

def can_query_channel(channel_id, user_id):
    if channel_id in ENV['id_channel']:
        return (
            (not ENV['channel_info'][channel_id]['is_private']) or
            (user_id in ENV['channel_info'][channel_id]['members'])
        )


def handle_query(event):
    """
    Handles a DM to the bot that is requesting a search of the archives.

    Usage:

        <query> from:<user> in:<channel> sort:asc|desc limit:<number>

        query: The text to search for.
        user: If you want to limit the search to one user, the username.
        channel: If you want to limit the search to one channel, the channel name.
        sort: Either asc if you want to search starting with the oldest messages,
            or desc if you want to start from the newest. Default asc.
        limit: The number of responses to return. Default 10.
    """
    try:
        text = []
        user = None
        channel = None
        sort = None
        limit = default_results_limit

        params = event['text'].lower().split()
        for p in params:
            # Handle emoji
            # usual format is " :smiley_face: "
            if len(p) > 2 and p[0] == ':' and p[-1] == ':':
                text.append(p)
                continue

            p = p.split(':')

            if len(p) == 1:
                text.append(p[0])
            if len(p) == 2:
                if p[0] == 'from':
                    user = get_user_id(p[1].replace('@','').strip())
                    if user is None:
                        raise ValueError('User %s not found' % p[1])
                if p[0] == 'in':
                    channel = get_channel_id(p[1].replace('#','').strip())
                    if channel is None:
                        raise ValueError('Channel %s not found' % p[1])
                if p[0] == 'sort':
                    if p[1] in ['asc', 'desc']:
                        sort = p[1]
                    else:
                        raise ValueError('Invalid sort order %s' % p[1])
                if p[0] == 'limit':
                    try:
                        limit = int(p[1])
                    except:
                        raise ValueError('%s not a valid number' % p[1])
                else:
                    raise ValueError('%s:%s is not a valid option for this command' % (p[0], [1]))

        query = 'SELECT message,user,timestamp,channel,hash FROM messages WHERE message LIKE (?)'
        query_args=["%"+" ".join(text)+"%"]

        if user:
            query += ' AND user=(?)'
            query_args.append(user)
        if channel:
            query += ' AND channel=(?)'
            query_args.append(channel)
        if sort:
            query += ' ORDER BY timestamp %s' % sort
            #query_args.append(sort)

        logger.debug(query)
        logger.debug(query_args)

        cursor.execute(query,query_args)

        res = cursor.fetchmany(limit)
        res_message=None
        results=None
        if res:
            logger.debug(res)
            results = {
                str(idx + 1): {
                    'message': i[0],
                    'user': i[1],
                    'formatted_user': get_user_name(i[1]),
                    'time': i[2],
                    'formatted_time': convert_timestamp(i[2]),
                    'channel': i[3],
                    'formatted_channel': '#' + get_channel_name(i[3]),
                    'hash': i[4],
                }
                for idx, i in enumerate(res) if can_query_channel(i[3], event['user'])
            }
            res_message = '\n\n\n'.join(['[%s] %s (@%s, %s, %s, %s)' % (
                idx, i['message'], i['formatted_user'], i['formatted_time'], i['formatted_channel'], i['hash'][:8]
            ) for idx, i in results.items()])
        if res_message:
            send_message(res_message, event['channel'])
        else:
            send_message('No results found', event['channel'])
        save_user_query(event, results)

    except ValueError as e:
        logger.error(traceback.format_exc())
        send_message(str(e), event['channel'])


def save_user_query(event, results):
    """
    Saves a user query so we can refer back to it if they ask for more info. Stores the results so we know what they
    mean when they give us an index.
    """

    if results:
        cursor.execute('INSERT INTO searches(query, user, results, timestamp) VALUES(?, ?, ?, ?)',
                       (event['text'], event['user'], json.dumps(results), event['ts'])
                       )
        conn.commit()

def get_previous_user_query(user):
    """
    Saves a user query so we can refer back to it if they ask for more info. Stores the results so we know what they
    mean when they give us an index.
    """
    logger.debug(user)
    cursor.execute('SELECT user, results FROM searches WHERE user = (?) ORDER BY timestamp DESC LIMIT 1', (user, ))
    res = cursor.fetchone()
    return {
        'user': res[0],
        'results': json.loads(res[1]) if res[1] else {},
    }


def handle_more_query(event):
    """
    Handles a DM to the bot that is requesting context around a message from a previous search.

    Usage:

        <query> from:<user> in:<channel> sort:asc|desc limit:<number>

        query: The text to search for.
        user: If you want to limit the search to one user, the username.
        channel: If you want to limit the search to one channel, the channel name.
        sort: Either asc if you want to search starting with the oldest messages,
            or desc if you want to start from the newest. Default asc.
        limit: The number of responses to return. Default 10.
    """
    try:
        text = []
        user = None
        channel = None
        sort = None
        start_date = None
        end_date = None
        before_num_messages = default_messages_before
        after_num_messages = default_messages_after
        before_time_minutes_limit = None
        after_time_minutes_limit = None
        message_hash = None
        # limit = default_messages_limit

        matches = re.search(
            r'!more (?P<index>([\d]+\b|[a-fA-F0-9-]{6,}))( \+(?P<after>[\d.]+[a-zA-Z]*)| -(?P<before>[\d.]+[a-zA-Z]*))*',
            event['text'])
        if not matches:
            raise ValueError(
                'Invalid query. Query format is `!more <id> [-<number of messages before or time period before> [+<number of messages after or time period after>]]`. \n Other commands to filter the results are also available. Type `!help` to see all options.')
        else:
            if matches['index']:
                if len(matches['index']) >= 6:
                    message_hash = matches['index']
                else:
                    try:
                        # indexes are strings as it was stored in json in the db,
                        # but we want to know if it's an index from the previous search or a hash.
                        message_index = str(int(matches['index'], 10))
                    except:
                        raise ValueError(
                            'Message id must be either the message hash (min 6 charaters), or an index from your previous search.')
                    prev_search = get_previous_user_query(event['user'])
                    prev_results = prev_search['results']
                    logger.debug(('prev_results', prev_results))
                    if message_index not in prev_results:
                        raise ValueError(
                            "Could not find message index %s in your previous search's results." % message_index)
                    message_hash = prev_results[message_index]['hash']
            else:
                raise ValueError('No message id provided')

            if matches['after']:
                try:
                    after_num_messages = int(matches['after'], 10)
                except:
                    # isn't # of messages
                    after_time_minutes_limit = parse_time_to_minutes(matches['after'])
                    after_num_messages = None
            if matches['before']:
                try:
                    before_num_messages = int(matches['before'], 10)
                except:
                    # isn't # of messages
                    before_time_minutes_limit = parse_time_to_minutes(matches['before'])
                    before_num_messages = None

        params = event['text'].lower().split()
        for p in params:
            # Handle emoji
            # usual format is " :smiley_face: "
            if len(p) > 2 and p[0] == ':' and p[-1] == ':':
                text.append(p)
                continue

            p = p.split(':')

            if len(p) == 1:
                pass


            if len(p) == 2:
                if p[0] == 'from':
                    user = get_user_id(p[1].replace('@','').strip())
                    if user is None:
                        raise ValueError('User %s not found' % p[1])
                if p[0] == 'sort':
                    if p[1] in ['asc', 'desc']:
                        sort = p[1]
                    else:
                        raise ValueError('Invalid sort order %s' % p[1])
                # if p[0] == 'limit':
                #     try:
                #         limit = int(p[1])
                #     except:
                #         raise ValueError('%s not a valid number' % p[1])
                if p[0] == 'start':
                    try:
                        start_date = datetime.datetime.strptime(p[1], '%Y-%m-%d')
                    except:
                        raise ValueError('%s not a valid start date. Please use ISO format YYYY-MM-DD.' % p[1])
                if p[0] == 'end':
                    try:
                        end_date = datetime.datetime.strptime(p[1], '%Y-%m-%d')
                    except:
                        raise ValueError('%s not a valid end date. Please use ISO format YYYY-MM-DD.' % p[1])
                else:
                    raise ValueError('%s:%s is not a valid option for this command' % (p[0], [1]))

        if not message_hash:
            raise ValueError('Message `%s` not found' % message_hash)

        reference_message_query = "SELECT timestamp, hash, channel FROM messages where HASH like (?)"
        cursor.execute(reference_message_query, (message_hash + '%', ))
        reference_message_result = cursor.fetchone()

        if not reference_message_result:
            raise ValueError('Message `%s` not found in search' % message_hash)

        ref_message = {
            'timestamp': reference_message_result[0],
            'hash': reference_message_result[1],
            'channel': reference_message_result[2],
        }

        query = 'SELECT message,user,timestamp,channel,hash FROM messages m WHERE 1=1'
        query_args=[]

        query += " AND channel=(?)"
        query_args.append(ref_message['channel'])
        if user:
            query += ' AND user=(?)'
            query_args.append(user)
        if start_date:
            query += ' AND timestamp > (?)'
            query_args.append(start_date)
        if end_date:
            query += ' AND timestamp < (?)'
            query_args.append(end_date)

        # if we have the before or after, use the query to restrict.
        # we also create a backup query that will include all items
        # before/after (depends which one) that way this can be
        # mixed-and-matched with the date version. It's only used if
        # the before_after_sql is.
        before_after_sql = []

        backup_before_after = []
        backup_before_after_query_args = []

        if before_num_messages:
            before_after_sql += [""" 
            hash in (
                SELECT mb.hash 
                FROM messages mb
                WHERE (mb.timestamp - (?)) > 0
                AND mb.hash <> (?)
                ORDER BY mb.timestamp desc
                LIMIT (?)
            )
            """]
            query_args.append(ref_message['timestamp'])
            query_args.append(ref_message['hash'])
            query_args.append(before_num_messages)
        else:
            backup_before_after += ['timestamp < (?)']
            backup_before_after_query_args.append(ref_message['timestamp'])


        if after_num_messages:
            # message time minus ref time is positive - message after ref
            # message time minus ref time is negative - message before ref
            # message asc - early message first
            # message desc - last message first
            before_after_sql += [""" 
            hash in (
                SELECT mb.hash 
                FROM messages mb
                WHERE (mb.timestamp - (?)) > 0
                AND mb.hash <> (?)
                ORDER BY mb.timestamp asc
                LIMIT (?)
            )
            """]
            query_args.append(ref_message['timestamp'])
            query_args.append(ref_message['hash'])
            query_args.append(after_num_messages)
        else:
            backup_before_after += ['timestamp > (?)']
            backup_before_after_query_args.append(ref_message['timestamp'])

        if before_after_sql:
            # include the reference message if we're doing before/after
            before_after_sql += ["hash = (?)"]
            query_args.append(ref_message['hash'])

            if backup_before_after:
                # use the backup if we have it. it only exists when we set one but not the other.
                before_after_sql += backup_before_after
                query_args += backup_before_after_query_args

            query += ' AND (' + " OR ".join(before_after_sql) + ')'


        if before_time_minutes_limit:
            query += ' AND timestamp > (?)'
            query_args.append(str(float(ref_message['timestamp']) - float(before_time_minutes_limit * 60)))

        if after_time_minutes_limit:
            query += ' AND timestamp < (?)'
            query_args.append(str(float(ref_message['timestamp']) + float(after_time_minutes_limit * 60)))

        if sort:
            query += ' ORDER BY timestamp %s' % sort
            #query_args.append(sort)

        logger.debug(query)
        logger.debug(query_args)

        cursor.execute(query,query_args)

        res = cursor.fetchall()
        res_message=None
        if res:
            logger.debug(res)
            results = {
                str(idx + 1): {
                    'message': i[0],
                    'user': i[1],
                    'formatted_user': get_user_name(i[1]),
                    'time': i[2],
                    'formatted_time': convert_timestamp(i[2]),
                    'channel': i[3],
                    'formatted_channel': '#' + get_channel_name(i[3]),
                    'hash': i[4],
                }
                for idx, i in enumerate(res) if can_query_channel(i[3], event['user'])
            }
            res_message = '\n\n\n'.join(['[%s] %s (@%s, %s, %s, %s)' % (
                idx, i['message'], i['formatted_user'], i['formatted_time'], i['formatted_channel'], i['hash'][:8]
            ) for idx, i in results.items()])
        if res_message:
            send_message(res_message, event['channel'])
        else:
            send_message('No results found', event['channel'])
    except ValueError as e:
        logger.error(traceback.format_exc())
        send_message(str(e), event['channel'])


def handle_message(event):
    if 'text' not in event:
        return
    if 'subtype' in event and event['subtype'] == 'bot_message':
        return

    logger.debug(event)

    # If it's a DM, treat it as a search query
    if event['channel'][0] == 'D':
        if event['text'][0:5] == '!more':
            handle_more_query(event)
        else:
            handle_query(event)
    elif 'user' not in event:
        logger.warn("No valid user. Previous event not saved")
    else: # Otherwise save the message to the archive.
        cursor.executemany('INSERT INTO messages VALUES(?, ?, ?, ?, ?)',
            [(event['text'], event['user'], event['channel'], event['ts'], get_event_hash(event))]
        )
        conn.commit()

    logger.debug("--------------------------")

update_database()

# Loop
if sc.rtm_connect(auto_reconnect=True):
    update_users()
    update_channels()
    logger.info('Archive bot online. Messages will now be recorded...')
    while sc.server.connected is True:
        try:
            for event in sc.rtm_read():
                if event['type'] == 'message':
                    handle_message(event)
                    if 'subtype' in event and event['subtype'] in ['group_leave']:
                        update_channels()
                elif event['type'] in ['group_joined', 'member_joined_channel', 'channel_created', 'group_left']:
                    update_channels()
        except WebSocketConnectionClosedException:
            sc.rtm_connect()
        except:
            logger.error(traceback.format_exc())
        time.sleep(1)
else:
    logger.error('Connection Failed, invalid token?')
