"""
Client wrapping the python Slack SDK.
Handles retry/backoff logic using the Singer framework annotations.
"""
import time

import backoff
import logging
import singer
from slack.errors import SlackApiError

LOGGER = singer.get_logger()

logging.getLogger('backoff').setLevel(logging.CRITICAL)

def log_backoff_attempt(details):
    LOGGER.info("ConnectionFailure detected, triggering backoff: %d try", details.get("tries"))

class SlackClient(object):

    def __init__(self, webclient, config):
        self.webclient = webclient
        self.config = config
        self._channels = {}

    def wait(err=None):
        if isinstance(err, SlackApiError):
            if err.response.data.get("error", "") == "ratelimited":
                delay = int(err.response.headers.get("Retry-After", "0"))
            else:
                raise err
            time.sleep(delay)

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=5,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def _request(self, method, **kwargs):
        return getattr(self.webclient, method)(**kwargs)

    def _pages(self, response, method, **kwargs):
        """
        Yields `response` and the cursor pages after it. Every page is requested through
        `_request`, so a rate limit on a later page is waited out and retried as well
        (iterating a SlackResponse directly doesn't retry and failed the whole sync).
        """
        while True:
            yield response
            cursor = (response.get('response_metadata') or {}).get('next_cursor')
            if not cursor:
                return
            response = self._request(method, cursor=cursor, **kwargs)

    def _paginate(self, method, **kwargs):
        return self._pages(self._request(method, **kwargs), method, **kwargs)

    def get_all_channels(self, types, exclude_archived):
        # Listed once per sync: every stream that loops over channels shares this list
        key = (types, exclude_archived)
        if key not in self._channels:
            self._channels[key] = [
                channel
                for page in self._paginate('conversations_list',
                                           exclude_archived=exclude_archived,
                                           types=types)
                for channel in page.get('channels', [])
            ]
        return self._channels[key]

    def get_channel(self, include_num_members, channel=None):
        page = self._request('conversations_info', channel=channel,
                             include_num_members=include_num_members)
        yield page.get('channel')

    def get_channel_members(self, channel):
        try:
            members_cursor = self._request('conversations_members', channel=channel)
        except SlackApiError as err:
            if err.response.data.get("error", "") == "fetch_members_failed":
                LOGGER.warning('Failed to fetch members for channel: {}'
                               .format(channel))
                return []
            else:
                raise err

        return self._pages(members_cursor, 'conversations_members', channel=channel)

    def get_messages(self, channel, oldest, latest):
        try:
            LOGGER.info(f"Getting messages for channel: {channel}, oldest: {oldest}, latest: {latest}")
            messages = self._request('conversations_history',
                                     channel=channel,
                                     oldest=oldest,
                                     latest=latest)
        except SlackApiError as err:
            if err.response.data.get("error", "") == "not_in_channel":
                # The tap config might dictate that archived channels should
                # be processed, but if the slackbot was not made a member of
                # those channels prior to archiving attempting to get the
                # messages will throw an error
                LOGGER.warning(
                    'Attempted to get messages for channel: {} that '
                    'slackbot is not in'.format(
                        channel
                    ))
                return None
            else:
                raise err

        LOGGER.info(f"Got messages, processing now {len(messages.get('messages', []))}")
        return self._pages(messages, 'conversations_history',
                           channel=channel, oldest=oldest, latest=latest)

    def get_thread(self, channel, ts, inclusive, oldest, latest):
        return self._paginate('conversations_replies',
                              channel=channel,
                              ts=ts,
                              inclusive=inclusive,
                              oldest=oldest,
                              latest=latest)

    def get_users(self, limit):
        return self._paginate('users_list', limit=limit)

    def get_user_groups(self, include_count, include_disabled, include_user):
        return [self._request('usergroups_list',
                              include_count=include_count,
                              include_disabled=include_disabled,
                              include_user=include_user)]

    def get_teams(self):
        return [self._request('team_info')]

    def get_files(self, ts_from, ts_to, page=1):
        return self._request('files_list', ts_from=ts_from, ts_to=ts_to, page=page)

    def get_remote_files(self, ts_from, ts_to):
        return self._paginate('files_remote_list', ts_from=ts_from, ts_to=ts_to)

    def join_channel(self, channel):
        return self._request('conversations_join', channel=channel)
