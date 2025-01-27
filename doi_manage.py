import os
import psycopg2
import requests
import subprocess
import sys
import time

import local_doi_manage_settings as settings

from lxml import etree as ElementTree

from libpkg.dbutils import uncompress_bitmap_values
from libpkg.metautils import export_to_datacite
from libpkg.unixutils import make_tempdir, remove_tempdir, sendmail


DEBUG = False


def on_crash(exctype, value, traceback):
    if DEBUG:
        sys.__excepthook__(exctype, value, traceback)
    else:
        print("{}: {}".format(exctype.__name__, value))


sys.excepthook = on_crash


def open_dataset_overview(dsid):
    resp = requests.get("https://rda.ucar.edu/datasets/" + dsid + "/metadata/dsOverview.xml")
    if resp.status_code != 200:
        raise RuntimeError("unable to download dataset overview: status code: {}".format(resp.status_code))

    return ElementTree.fromstring(resp.text)


def do_url_registration(doi, dsid, api_config, tdir):
    regfile = os.path.join(tdir, dsid + ".reg")
    url = "https://rda.ucar.edu/datasets/{}/".format(dsid);
    with open(regfile, "w") as f:
        f.write("doi=" + doi + "\n")
        f.write("url=" + url + "\n")

    f.close()
    # register the URL
    proc = subprocess.run("curl -s --user {user}:{password} -H 'Content-type: text/plain;charset=UTF-8' -X PUT --data-binary @{regfile} https://{host}/doi/{doi}".format(**api_config, doi=doi, regfile=regfile), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err = proc.stderr.decode("utf-8")
    if len(err) > 0:
        sendmail(
            settings.notifications['error'],
            "rdadoi@ucar.edu",
            "DOI Error",
            "Error while registering the URL for DOI/dsid: '{}/{}': '{}'".format(doi, config['identifier'], err),
            devel=DEBUG
        )

    out = proc.stdout.decode("utf-8")
    if out != "OK":
        sendmail(
            settings.notifications['error'],
            "rdadoi@ucar.edu",
            "DOI Error",
            "Unexpected response while registering the URL for DOI/dsid: '{}/{}': '{}'".format(doi, dsid, out),
            devel=DEBUG
        )

    # verify the registration
    proc = subprocess.run("curl -s --user {user}:{password} https://{host}/doi/{doi}".format(**api_config, doi=doi), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err = proc.stderr.decode("utf-8")
    if len(err) > 0:
        sendmail(
            settings.notifications['error'],
            "rdadoi@ucar.edu",
            "DOI Error",
            "Error while retrieving the registered URL for DOI/dsid: '{}/{}': '{}'".format(doi, dsid, err),
            devel=DEBUG
        )

    out = proc.stdout.decode("utf-8")
    if out != url:
        sendmail(
            settings.notifications['error'],
            "rdadoi@ucar.edu",
            "DOI Error",
            "Unexpected response while retrieving the registered URL for DOI/dsid: '{}/{}': '{}'".format(doi, dsid, out),
            devel=DEBUG
        )


def create_doi(config):
    print("create_doi " + str(config))
    if config['api_config']['caller'] == "operations":
        test_config = config.copy()
        test_config['api_config'] = settings.test_api_config
        out, warn = create_doi(test_config)
        if len(warn) > 0:
            raise RuntimeError("failed test run: '{}'".format(warn))

    try:
        metadb_conn = psycopg2.connect(**settings.metadb_config)
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'".format(err))

    try:
        wagtaildb_conn = psycopg2.connect(**settings.wagtaildb_config)
    except psycopg2.Error as err:
        raise RuntimeError("wagtail database connection error: '{}'".format(err))

    tdir = make_tempdir("/tmp")
    if len(tdir) == 0:
        raise FileNotFoundError("unable to create a temporary directory")

    try:
        metadb_cursor = metadb_conn.cursor()
        wagtaildb_cursor = wagtaildb_conn.cursor()
        metadb_cursor.execute("select type from search.datasets where dsid = %s", (config['identifier'], ))
        res = metadb_cursor.fetchone()
    except psycopg2.Error as err:
        raise RuntimeError("metadata database connection error: '{}'".format(err))
    else:
        if res is None:
            raise RuntimeError("dataset '{}' not found".format(config['identifier']))

        if res[0] not in ("P", "H"):
            raise RuntimeError("a DOI can only be assigned to a dataset typed as 'primary' or 'historical'")

        root = open_dataset_overview(config['identifier'])
        dc, warn = export_to_datacite(config, root, metadb_cursor, wagtaildb_cursor)

        # mint the DOI and send the associated metadata
        print(tdir)
        dcfile = os.path.join(tdir, config['identifier'] + ".dc4")
        with open(dcfile, "w") as f:
            f.write(dc)

        f.close()
        proc = subprocess.run("curl -s --user {user}:{password} -H 'Content-type: application/xml;charset=UTF-8' -X PUT -d@{dcfile} https://{host}/metadata/{doi_prefix}".format(**config['api_config'], dcfile=dcfile), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        err = proc.stderr.decode("utf-8")
        if len(err) > 0:
            raise RuntimeError("error while creating DOI: '{}'".format(err))

        out = proc.stdout.decode("utf-8")
        parts = out.split()
        if len(parts) != 2 or parts[0] != "OK":
            raise RuntimeError("unexpected response while creating DOI: '{}'".format(out))

        doi = parts[-1][1:-1]
        out = ["Success: " + doi]

        # register the dereferencing URL for the DOI
        time.sleep(5)
        do_url_registration(doi, config['identifier'], config['api_config'], tdir)
        if config['api_config']['caller'] == "operations":
            out.append("View the DOI at https://commons.datacite.org/?query=" + doi)

    finally:
        remove_tempdir(tdir)
        metadb_conn.close()
        wagtaildb_conn.close()

    return ("\n".join(out), warn)


def update_doi(config, **kwargs):
    print("update_doi " + str(kwargs['retire']) + " " + str(config))
    return ""


if __name__ == "__main__":
    if len(sys.argv[1:]) < 3:
        print((
            "usage: {} <authorization_key> [options...] <mode> <identifier>".format(sys.argv[0][sys.argv[0].rfind("/")+1:]) + "\n"
            "\nmode (must be one of the following):\n"
            "    create <dnnnnnn>   register a new DOI for dataset dnnnnnn\n"
            "    update <DOI>       update the DataCite metadata for an existing DOI\n"
            "    supersede <DOI>    mark the DOI as being superseded by another DOI\n"
            "    terminate <DOI>    mark the DOI as 'dead'\n"
            "\noptions:\n"
            "    --debug  show stack trace for an exception\n"
            "    -t       run in test mode\n"
            "    -v3      push DataCite version 3 metadata\n"
        ))
        sys.exit(1)

    args = sys.argv[1:]
    auth_key = args[0]
    del args[0]
    identifier = args[-1]
    del args[-1]
    mode = args[-1]
    del args[-1]
    print(auth_key)
    print(args)
    print(mode)
    print(identifier)

    config = {'identifier': identifier}
    if "--debug" in args:
        DEBUG = True

    if "-t" in args:
        config.update({'api_config': settings.test_api_config})
    else:
        config.update({'api_config': settings.operations_api_config})

    if "-v3" in args:
        config.update({'datacite_version': "3"})
    else:
        config.update({'datacite_version': settings.default_datacite_version})

    if mode == "create":
        out, warn = create_doi(config)
    elif mode == "update":
        warn = update_doi(config, retire=False)
    elif mode in ("supersede", "terminate"):
        warn = update_doi(config, retire=True)
    else:
        raise ValueError("invalid mode")
        sys.exit(1)

    if len(warn) > 0:
        print("Warning(s):\n{}".format(warn))

    if 'out' in locals() and len(out) > 0:
        print(out)

    sys.exit(0)
