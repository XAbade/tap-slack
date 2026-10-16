import singer

def generate_catalog(streams):

    catalog = {}
    catalog['streams'] = []
    for stream in streams:
        schema = stream.load_schema()
        mdata = singer.metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream.key_properties,
            valid_replication_keys=stream.valid_replication_keys,
            replication_method=stream.replication_method)
        if stream.replication_key:
            mdata = singer.metadata.to_list(singer.metadata.write(
                singer.metadata.to_map(mdata), (), 'replication-key', stream.replication_key))
        catalog_entry = {
            'stream': stream.name,
            'tap_stream_id': stream.name,
            'schema': schema,
            'metadata': mdata
        }
        catalog['streams'].append(catalog_entry)

    return catalog
