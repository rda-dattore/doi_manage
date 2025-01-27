test_api_config = {
    'user': "<datacite_test_user>",
    'password': "<datacite_test_password>",
    'host': "mds.test.datacite.org",
    'doi_prefix': "<datacite_test_doi_prefix>",
    'caller': "test",
}
operations_api_config = {
    'user': "<datacite_operations_user>",
    'password': "<datacite_operations_password>",
    'host': "mds.datacite.org",
    'doi_prefix': "<datacite_operations_doi_prefix>",
    'caller': "operations",
}

managed_prefixes = [
    "<prefix>",
]

default_datacite_version = "4"

metadb_config = {
    'user': "<metadb_user>",
    'password': "<metadb_password>",
    'host': "<metadb_host>",
    'dbname': "<wagtaildb_dbname>",
}

wagtaildb_config = {
    'user': "<wagtaildb_user>",
    'password': "<wagtaildb_password>",
    'host': "<wagtaildb_host>",
    'dbname': "<wagtaildb_dbname>",
}

notifications = {
    'info': [
        "<email_address>",
    ],
    'error': [
        "<email_address>",
    ],
}
