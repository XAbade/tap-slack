import os
from datetime import timedelta

import singer
from singer import metadata, utils
from singer.utils import strptime_to_utc

from tap_slack.transform import transform_json

LOGGER = singer.get_logger()
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


class SlackStream:
    replication_key = None

    def __init__(self, client, config=None, catalog=None, state=None, write_to_singer=True):
        self.client = client
        self.config = config
        self.catalog = catalog
        self.state = state
        self.write_to_singer = write_to_singer
        if config:
            self.date_window_size = int(config.get('date_window_size', '7'))
        else:
            self.date_window_size = 7

    @staticmethod
    def get_abs_path(path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self):
        schema_path = self.get_abs_path('schemas')
        # pylint: disable=no-member
        return singer.utils.load_json('{}/{}.json'.format(schema_path, self.name))

    def write_schema(self):
        schema = self.load_schema()
        # pylint: disable=no-member
        return singer.write_schema(stream_name=self.name, schema=schema,
                                   key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    def update_bookmarks(self, stream, value):
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}
        self.state['bookmarks'][stream] = value
        LOGGER.info('Stream: {} - Write state, bookmark value: {}'.format(stream, value))
        self.write_state()

    def get_bookmark(self, stream, default):
        # default only populated on initial sync
        if (self.state is None) or ('bookmarks' not in self.state):
            return default
        return self.state.get('bookmarks', {}).get(stream, default)

    def get_absolute_date_range(self, start_date):
        """
        Based on parameters in tap configuration, returns the absolute date range for the sync,
        including the lookback window if applicable.
        :param start_date: The start date in the config, or the last synced date from the bookmark
        :return: the start date and the end date that make up the date range
        """
        lookback_window = self.config.get('lookback_window', '14')
        start_dttm = strptime_to_utc(start_date)
        attribution_window = int(lookback_window)
        now_dttm = utils.now()
        delta_days = (now_dttm - start_dttm).days
        if delta_days < attribution_window:
            start_ddtm = now_dttm - timedelta(days=attribution_window)
        else:
            start_ddtm = start_dttm

        return start_ddtm, now_dttm

    def date_windows(self, start, end):
        """
        Splits the sync range into windows of `date_window_size` days.
        :return: (window_start, window_end) pairs covering start -> end
        """
        window_start = start
        while window_start < end:
            window_end = min(window_start + timedelta(days=self.date_window_size), end)
            yield window_start, window_end
            window_start = window_end

    def _all_channels(self):
        types = "public_channel"
        enable_private_channels = self.config.get("private_channels", "false")
        exclude_archived = self.config.get("exclude_archived", "false")
        if enable_private_channels == "true":
            types = "public_channel,private_channel"

        conversations_list = self.client.get_all_channels(types=types,
                                                          exclude_archived=exclude_archived)

        for page in conversations_list:
            channels = page.get('channels')
            for channel in channels:
                yield channel

    def _specified_channels(self):
        for channel_id in self.config.get("channels"):
            yield self.client.get_channel(include_num_members=0, channel=channel_id)

    def channels(self):
        if "channels" in self.config:
            yield from self._specified_channels()
        else:
            yield from self._all_channels()


# ConversationsStream = Slack Channels
class ConversationsStream(SlackStream):
    name = 'channels'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    date_fields = ['created']

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversations') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                channels = self.channels()
                for channel in channels:
                    transformed_channel = transform_json(stream=self.name, data=[channel],
                                                         date_fields=self.date_fields)
                    with singer.Transformer(
                            integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                            as transformer:
                        transformed_record = transformer.transform(data=transformed_channel[0], schema=schema,
                                                                   metadata=metadata.to_map(mdata))
                        if self.write_to_singer:
                            singer.write_record(stream_name=self.name,
                                                time_extracted=singer.utils.now(),
                                                record=transformed_record)
                            counter.increment()


# ConversationsMembersStream = Slack Channel Members (Users)
class ConversationMembersStream(SlackStream):
    name = 'channel_members'
    key_properties = ['channel_id', 'user_id']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    date_fields = []

    def sync(self, mdata):

        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversation_members') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    channel_id = channel.get('id')

                    members_cursor = self.client.get_channel_members(channel_id)

                    for page in members_cursor:
                        members = page.get('members')
                        for member in members:
                            data = {'channel_id': channel_id, 'user_id': member}
                            with singer.Transformer() as transformer:
                                transformed_record = transformer.transform(data=data, schema=schema,
                                                                           metadata=metadata.to_map(
                                                                               mdata))
                                if self.write_to_singer:
                                    singer.write_record(stream_name=self.name,
                                                        time_extracted=singer.utils.now(),
                                                        record=transformed_record)
                                    counter.increment()


# ConversationsHistoryStream = Slack Messages (not including reply threads)
class ConversationHistoryStream(SlackStream):
    name = 'messages'
    key_properties = ['channel_id', 'ts']
    replication_method = 'INCREMENTAL'
    forced_replication_method = 'INCREMENTAL'
    replication_key = 'ts'
    valid_replication_keys = ['ts']
    date_fields = ['ts']

    # pylint: disable=arguments-differ
    def update_bookmarks(self, channel_id, value):
        """
        For the messages stream, bookmarks are written per-channel.
        :param channel_id: The channel to bookmark
        :param value: The earliest message date in the window.
        :return: None
        """
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}
        if self.name not in self.state['bookmarks']:
            self.state['bookmarks'][self.name] = {}
        self.state['bookmarks'][self.name][channel_id] = value
        self.write_state()

    # pylint: disable=arguments-differ
    def get_bookmark(self, channel_id, default):
        """
        Gets the channel's bookmark value, if present, otherwise a default value passed in.
        :param channel_id: The channel to retrieve the bookmark for.
        :param default: The default value to return if no bookmark
        :return: The bookmark or default value passed in
        """
        # default only populated on initial sync
        if (self.state is None) or ('bookmarks' not in self.state):
            return default
        return self.state.get('bookmarks', {}).get(self.name, {channel_id: default}) \
            .get(channel_id, default)

    # pylint: disable=too-many-branches,too-many-statements
    def sync(self, mdata):

        schema = self.load_schema()
        threads_stream = None
        threads_mdata = None

        # If threads are also being synced we'll need to do that for each message
        for catalog_entry in self.catalog.get_selected_streams(self.state):
            if catalog_entry.stream == 'threads':
                threads_mdata = catalog_entry.metadata
                threads_stream = ThreadsStream(client=self.client, config=self.config,
                                               catalog=self.catalog, state=self.state)
                threads_stream.write_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversation_history') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    channel_id = channel.get('id')

                    bookmark_date = self.get_bookmark(channel_id, self.config.get('start_date'))
                    start, end = self.get_absolute_date_range(bookmark_date)

                    for date_window_start, date_window_end in self.date_windows(start, end):

                        messages = self.client \
                            .get_messages(channel=channel_id,
                                          oldest=int(date_window_start.timestamp()),
                                          latest=int(date_window_end.timestamp()))

                        if messages is None:
                            # The bot can't read this channel, skip it and keep its bookmark
                            break

                        for page in messages:
                            messages = page.get('messages')
                            transformed_messages = transform_json(stream=self.name,
                                                                  data=messages,
                                                                  date_fields=self.date_fields,
                                                                  channel_id=channel_id)
                            for message in transformed_messages:
                                data = {'channel_id': channel_id}
                                data = {**data, **message}

                                # If threads are being synced then the message data for the
                                # message the threaded replies are in response to will be
                                # synced to the messages table as well as the threads table
                                if threads_stream:
                                    # If threads is selected we need to sync all the
                                    # threaded replies to this message
                                    try:
                                        if data.get('reply_count'):
                                            threads_stream.sync(mdata=threads_mdata,
                                                                channel_id=channel_id,
                                                                ts=data.get('thread_ts'),
                                                                start=start,
                                                                end=end)
                                        else:
                                            # Without replies the thread is just this message,
                                            # so skip the (rate limited) conversations.replies call
                                            threads_stream.write_messages(
                                                mdata=threads_mdata,
                                                channel_id=channel_id,
                                                messages=[{**message,
                                                           'ts': data.get('thread_ts')}])
                                        threads_stream.write_state()
                                    except:
                                        #Skip the thread on error
                                        LOGGER.exception(
                                            f"Failed to sync thread {data.get('thread_ts')}"
                                        )

                                with singer.Transformer(
                                        integer_datetime_fmt=
                                        "unix-seconds-integer-datetime-parsing"
                                ) as transformer:
                                    transformed_record = transformer.transform(
                                        data=data,
                                        schema=schema,
                                        metadata=metadata.to_map(mdata)
                                    )
                                    record_timestamp = \
                                        transformed_record.get('thread_ts', '').partition('.')[
                                            0]
                                    record_timestamp_int = int(record_timestamp)
                                    if record_timestamp_int >= start.timestamp():
                                        if self.write_to_singer:
                                            singer.write_record(stream_name=self.name,
                                                                time_extracted=singer.utils.now(),
                                                                record=transformed_record)
                                            counter.increment()

                        # Slack returns every message between oldest and latest, so once a
                        # window is done the channel's bookmark can move to its end
                        self.update_bookmarks(channel_id,
                                              date_window_end.strftime(DATETIME_FORMAT))


# UsersStream = Slack Users
class UsersStream(SlackStream):
    name = 'users'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated'
    valid_replication_keys = ['updated']
    date_fields = ['updated']

    def sync(self, mdata):

        schema = self.load_schema()
        bookmark = singer.get_bookmark(state=self.state, tap_stream_id=self.name,
                                       key=self.replication_key)
        if bookmark is None:
            bookmark = self.config.get('start_date')
        new_bookmark = bookmark

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_users') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                users_list = self.client.get_users(limit=100)

                for page in users_list:
                    users = page.get('members')
                    transformed_users = transform_json(stream=self.name, data=users,
                                                       date_fields=self.date_fields)
                    for user in transformed_users:
                        with singer.Transformer(
                                integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                                as transformer:
                            transformed_record = transformer.transform(data=user, schema=schema,
                                                                       metadata=metadata.to_map(
                                                                           mdata))
                            new_bookmark = max(new_bookmark, transformed_record.get('updated'))
                            if transformed_record.get('updated') > bookmark:
                                if self.write_to_singer:
                                    singer.write_record(stream_name=self.name,
                                                        time_extracted=singer.utils.now(),
                                                        record=transformed_record)
                                    counter.increment()

        self.state = singer.write_bookmark(state=self.state, tap_stream_id=self.name,
                                           key=self.replication_key, val=new_bookmark)


# ThreadsStream = Slack Message Threads (Replies to Slack message)
# Threads are synced from the messages stream: for each message in the messages sync range the
# thread is requested, limited to replies in that same range. This makes threads incremental on
# `ts` through the per-channel messages bookmark. A thread is only synced if the message it is
# started on fits within that range (plus the lookback window).
class ThreadsStream(SlackStream):
    name = 'threads'
    key_properties = ['channel_id', 'ts', 'thread_ts']
    replication_method = 'INCREMENTAL'
    replication_key = 'ts'
    valid_replication_keys = ['ts']
    date_fields = ['ts', 'last_read']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # A thread can be requested more than once per sync (e.g. via its parent message and
        # again via a reply broadcast to the channel), so track what was already written.
        self.synced_keys = set()

    def sync(self, mdata, channel_id, ts, start=None, end=None):
        schema = self.load_schema()
        if start is None or end is None:
            start, end = self.get_absolute_date_range(self.config.get('start_date'))

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_threads') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                replies = self.client.get_thread(channel=channel_id,
                                                 ts=ts,
                                                 inclusive="true",
                                                 oldest=int(start.timestamp()),
                                                 latest=int(end.timestamp()))

                for page in replies:
                    self.write_messages(mdata=mdata, channel_id=channel_id,
                                        messages=page.get('messages', []), counter=counter,
                                        schema=schema)

    def write_messages(self, mdata, channel_id, messages, counter=None, schema=None):
        """
        Writes thread messages as returned by conversations.replies (raw Slack `ts`).
        """
        schema = schema or self.load_schema()
        transformed_threads = transform_json(stream=self.name,
                                             data=messages,
                                             date_fields=self.date_fields,
                                             channel_id=channel_id)
        for message in transformed_threads:
            with singer.Transformer(
                    integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                    as transformer:
                transformed_record = transformer.transform(data=message, schema=schema,
                                                           metadata=metadata.to_map(mdata))
                key = tuple(transformed_record.get(k) for k in self.key_properties)
                if key in self.synced_keys:
                    continue
                self.synced_keys.add(key)
                if self.write_to_singer:
                    singer.write_record(stream_name=self.name,
                                        time_extracted=singer.utils.now(),
                                        record=transformed_record)
                    if counter:
                        counter.increment()


# UserGroupsStream = Slack User Groups
class UserGroupsStream(SlackStream):
    name = 'user_groups'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'date_update'
    valid_replication_keys = ['date_update']

    def sync(self, mdata):
        schema = self.load_schema()
        # usergroups.list can't filter by date, so filter on `date_update` here
        bookmark = singer.get_bookmark(state=self.state, tap_stream_id=self.name,
                                       key=self.replication_key)
        if bookmark is None:
            bookmark = self.config.get('start_date')
        bookmark_dttm = strptime_to_utc(bookmark)
        new_bookmark_dttm = bookmark_dttm

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_user_groups') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                usergroups_list = self.client.get_user_groups(include_count="true",
                                                              include_disabled="true",
                                                              include_user="true")

                for page in usergroups_list:
                    for usergroup in page.get('usergroups'):
                        with singer.Transformer(
                                integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                                as transformer:
                            transformed_record = transformer.transform(data=usergroup,
                                                                       schema=schema,
                                                                       metadata=metadata.to_map(
                                                                           mdata))
                            date_update = transformed_record.get(self.replication_key)
                            if date_update:
                                date_update_dttm = strptime_to_utc(date_update)
                                if date_update_dttm <= bookmark_dttm:
                                    continue
                                new_bookmark_dttm = max(new_bookmark_dttm, date_update_dttm)
                            if self.write_to_singer:
                                singer.write_record(stream_name=self.name,
                                                    time_extracted=singer.utils.now(),
                                                    record=transformed_record)
                                counter.increment()

        self.state = singer.write_bookmark(state=self.state, tap_stream_id=self.name,
                                           key=self.replication_key,
                                           val=singer.utils.strftime(new_bookmark_dttm))


# TeamsStream = Slack Teams
class TeamsStream(SlackStream):
    name = 'teams'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    date_fields = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='team_info') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                team_info = self.client.get_teams()

                for page in team_info:
                    team = page.get('team')
                    with singer.Transformer(
                            integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                            as transformer:
                        transformed_record = transformer.transform(data=team,
                                                                   schema=schema,
                                                                   metadata=metadata.to_map(
                                                                       mdata))
                        if self.write_to_singer:
                            singer.write_record(stream_name=self.name,
                                                time_extracted=singer.utils.now(),
                                                record=transformed_record)
                            counter.increment()


class FilesBaseStream(SlackStream):
    """
    Shared sync for files and remote files. Both endpoints filter on the file's creation time
    (`ts_from`/`ts_to`), so `timestamp` is the replication key and the bookmark moves to the end
    of each date window once it's fully synced.
    """
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'timestamp'
    valid_replication_keys = ['timestamp']
    date_fields = []

    def get_pages(self, ts_from, ts_to):
        raise NotImplementedError

    def sync(self, mdata):
        schema = self.load_schema()
        synced_ids = set()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_files') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                bookmark_date = self.get_bookmark(self.name, self.config.get('start_date'))
                start, end = self.get_absolute_date_range(bookmark_date)

                for date_window_start, date_window_end in self.date_windows(start, end):
                    pages = self.get_pages(ts_from=int(date_window_start.timestamp()),
                                           ts_to=int(date_window_end.timestamp()))
                    for page in pages:
                        files = transform_json(stream=self.name,
                                               data=page.get('files', []),
                                               date_fields=self.date_fields)
                        for file in files:
                            # ts_from/ts_to are inclusive, so a file on a window boundary
                            # comes back twice
                            if file.get('id') in synced_ids:
                                continue
                            if int(file.get('timestamp', 0)) < start.timestamp():
                                continue
                            synced_ids.add(file.get('id'))
                            with singer.Transformer(
                                    integer_datetime_fmt="unix-seconds-integer-datetime-parsing"
                            ) as transformer:
                                transformed_record = transformer.transform(
                                    data=file,
                                    schema=schema,
                                    metadata=metadata.to_map(mdata)
                                )
                                if self.write_to_singer:
                                    singer.write_record(stream_name=self.name,
                                                        time_extracted=singer.utils.now(),
                                                        record=transformed_record)
                                    counter.increment()

                    self.update_bookmarks(self.name, date_window_end.strftime(DATETIME_FORMAT))


# FilesStream = Files uploaded/shared to Slack and hosted by Slack
class FilesStream(FilesBaseStream):
    name = 'files'

    def get_pages(self, ts_from, ts_to):
        # files.list is paged by page number, not by cursor
        page_number = 1
        while True:
            response = self.client.get_files(ts_from=ts_from, ts_to=ts_to, page=page_number)
            yield response
            if page_number >= response.get('paging', {}).get('pages', 1):
                break
            page_number += 1


# RemoteFilesStream = Files shared to Slack but not hosted by Slack
class RemoteFilesStream(FilesBaseStream):
    name = 'remote_files'

    def get_pages(self, ts_from, ts_to):
        # files.remote.list is paged by cursor, which iterating the response handles
        yield from self.client.get_remote_files(ts_from=ts_from, ts_to=ts_to)


AVAILABLE_STREAMS = {
    "channels": ConversationsStream,
    "users": UsersStream,
    "channel_members": ConversationMembersStream,
    "messages": ConversationHistoryStream,
    "threads": ThreadsStream,
    "user_groups": UserGroupsStream,
    "teams": TeamsStream,
    "files": FilesStream,
    "remote_files": RemoteFilesStream,
}
