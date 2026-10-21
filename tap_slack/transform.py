from datetime import datetime, timezone


def decimal_timestamp_to_utc_timestamp(timestamp):
    # Some timestamps returned by the Slack API are in the format of `Seconds.Microseconds`.
    # Keep the microseconds: a message's `ts` is its unique id within a channel, so dropping
    # them makes different messages posted in the same second share the same primary key.
    seconds, _, fraction = timestamp.partition(".")
    parsed = datetime.fromtimestamp(int(seconds), tz=timezone.utc) \
        .replace(microsecond=int(fraction.ljust(6, "0")[:6]))
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def raw_ts(record):
    """Slack `ts` as delivered (`Seconds.Microseconds` string), or None once it has been converted."""
    value = record.get("ts")
    return value if isinstance(value, str) and value.replace(".", "", 1).isdigit() else None


def transform_json(stream, data, date_fields, channel_id=None):

    if data:
        for record in data:
            if stream == "messages":
                # Strip out file info and just keep id
                file_ids = []
                files = record.get("files", [])
                for file in files:
                    file_id = file.get("id", None)
                    if file_id:
                        file_ids.append(file_id)
                record['file_ids'] = file_ids

            if stream in ["messages", "threads"]:
                # add channel_id to the message
                record['channel_id'] = channel_id

            if stream == "channels":
                # These come back on the client response but aren't actually populated
                # Or in Slack's documentation at the time of writing, just remove them
                record.pop("parent_conversation", None)
                record.pop("channel_id", None)

            if stream in ["messages", "threads"]:
                # Slack sets `thread_ts` only on threaded messages: to the parent's `ts` on a
                # reply, to its own `ts` on a parent. Keep that link; give an unthreaded
                # message its own `ts` so `thread_ts` is never null (it is a key property of
                # `threads`). `ts` itself is converted to a datetime below.
                own_ts = raw_ts(record)
                if own_ts and not record.get("thread_ts"):
                    record["thread_ts"] = own_ts

            for date_field in date_fields:
                timestamp = record.get(date_field, None)
                if timestamp and isinstance(timestamp, str):
                    record[date_field] = decimal_timestamp_to_utc_timestamp(timestamp)
    return data
