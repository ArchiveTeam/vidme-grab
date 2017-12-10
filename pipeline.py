# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os.path
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string

sys.path.insert(0, os.getcwd())

import warcio
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter

if not warcio.__file__ == os.path.join(os.getcwd(), 'warcio', '__init__.pyc'):
    print('Warcio was not imported correctly.')
    print('Location: ' + warcio.__file__ + '.')
    sys.exit(1)

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable


# check the seesaw version
if StrictVersion(seesaw.__version__) < StrictVersion("0.8.5"):
    raise Exception("This pipeline needs seesaw version 0.8.5 or higher.")


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
WGET_LUA = find_executable(
    "Wget+Lua",
    ["GNU Wget 1.14.lua.20130523-9a5c", "GNU Wget 1.14.lua.20160530-955376b"],
    [
        "./wget-lua",
        "./wget-lua-warrior",
        "./wget-lua-local",
        "../wget-lua",
        "../../wget-lua",
        "/home/warrior/wget-lua",
        "/usr/bin/wget-lua"
    ]
)

if not WGET_LUA:
    raise Exception("No usable Wget+Lua found.")


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20171209.02"
USER_AGENT = 'ArchiveTeam'
TRACKER_ID = 'vidme2'
TRACKER_HOST = 'tracker.archiveteam.org'


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "CheckIP")
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 6:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, "PrepareDirectories")
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item["item_name"]
        escaped_item_name = item_name.replace(':', '_').replace('/', '_').replace('~', '_')
        dirname = "/".join((item["data_dir"], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item["item_dir"] = dirname
        item["warc_file_base"] = "%s-%s-%s" % (self.warc_prefix, escaped_item_name[:50],
            time.strftime("%Y%m%d-%H%M%S"))

        open("%(item_dir)s/%(warc_file_base)s.warc.gz" % item, "w").close()
        open("%(item_dir)s/%(warc_file_base)s_data.txt" % item, "w").close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        # NEW for 2014! Check if wget was compiled with zlib support
        if os.path.exists("%(item_dir)s/%(warc_file_base)s.warc" % item):
            raise Exception('Please compile wget with zlib support!')

        os.rename("%(item_dir)s/%(warc_file_base)s_data.txt" % item,
              "%(data_dir)s/%(warc_file_base)s_data.txt" % item)

        shutil.rmtree("%(item_dir)s" % item)


class DeduplicateWarc(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "DeduplicateWarc")

    def create_revisit_record(self, writer, record, data):
        warc_headers = record.rec_headers
        warc_headers.add_header('WARC-Refers-To', data['id'])
        warc_headers.replace_header('WARC-Refers-To-Date', data['date'])
        warc_headers.replace_header('WARC-Refers-To-Target-URI', data['url'])
        warc_headers.replace_header('WARC-Type', 'revisit')
        warc_headers.replace_header('WARC-Truncated', 'length')
        warc_headers.replace_header('WARC-Profile', 'http://netpreserve.org/warc/1.1/revisit/identical-payload-digest')
        warc_headers.remove_header('WARC-Block-Digest')
        warc_headers.remove_header('Content-Length')
        return writer.create_warc_record(
            record.rec_headers.get_header('WARC-Target-URI'),
            'revisit',
            warc_headers=warc_headers,
            http_headers=record.http_headers
        )

    def process_record(self, writer, record, out, records):
        if record.rec_headers.get_header('WARC-Type') == 'response':
            record_url = record.rec_headers.get_header('WARC-Target-URI')
            record_digest = record.rec_headers.get_header('WARC-Payload-Digest')
            record_date = record.rec_headers.get_header('WARC-Date')
            record_id = record.rec_headers.get_header('WARC-Record-ID')
            print('Deduplicating digest ' + record_digest + ', url ' + record_url)

            if record_digest in records:
                print('Found duplicate, writing revisit record.')
                return self.create_revisit_record(writer, record,
                                                  records[record_digest])
            else:
                records[record_digest] = {
                    'url': record_url,
                    'date': record_date,
                    'id': record_id
                }
                return record
        else:
            return record

    def process(self, item):
        records = {}
        num_records = 0

        filename_in = '%(item_dir)s/%(warc_file_base)s.warc.gz' % item
        filename_out = '%(data_dir)s/%(warc_file_base)s.warc.gz' % item

        with open(filename_in, 'rb') as file_in:
            with open(filename_out, 'wb') as file_out:
                writer = WARCWriter(filebuf=file_out, gzip=True)
                for record in ArchiveIterator(file_in):
                    num_records += 1
                    writer.write_record(self.process_record(writer, record,
                                                            filename_out,
                                                            records))

        print('Processed {} payloads, found {} unique payloads.'
              .format(num_records, len(records)))

def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()

CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'vidme.lua'))

def stats_id_function(item):
    # NEW for 2014! Some accountability hashes and stats.
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_LUA,
            "-U", USER_AGENT,
            "-nv",
            "--no-cookies",
            "--content-on-error",
            "--lua-script", "vidme.lua",
            "-o", ItemInterpolation("%(item_dir)s/wget.log"),
            "--no-check-certificate",
            "--output-document", ItemInterpolation("%(item_dir)s/wget.tmp"),
            "--truncate-output",
            "-e", "robots=off",
            "--rotate-dns",
            "--recursive", "--level=inf",
            "--no-parent",
            "--page-requisites",
            "--timeout", "30",
            "--tries", "inf",
            "--domains", "vid.me",
            "--span-hosts",
            "--waitretry", "30",
            "--warc-file", ItemInterpolation("%(item_dir)s/%(warc_file_base)s"),
            "--warc-header", "operator: Archive Team",
            "--warc-header", "vidme-dld-script-version: " + VERSION,
            "--warc-header", ItemInterpolation("vidme-item: %(item_name)s"),
        ]

        item_name = item['item_name']
        assert ':' in item_name
        item_type, item_value = item_name.split(':', 1)

        item['item_type'] = item_type
        item['item_value'] = item_value

        if item_type == 'video':
            wget_args.extend(['--warc-header', 'vidme-video-id: {i}'.format(i=item_value)])
            wget_args.append('https://api.vid.me/video/{i}'.format(i=item_value))
            wget_args.append('https://api.vid.me/video/{i}/upnext'.format(i=item_value))
            wget_args.append('https://api.vid.me/video/{i}/likes?offset=0&limit=10'.format(i=item_value))
            wget_args.append('https://api.vid.me/video/{i}/comments?offsetAtParentLevel=true&order=score&offset=0&limit=20'.format(i=item_value))
        else:
            raise Exception('Unknown item')

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title = "vidme",
    project_html = """
    <img class="project-logo" alt="logo" src="http://archiveteam.org/images/e/e2/Vidme_logo.png" />
    <h2>vid.me <span class="links"><a href="https://vid.me/">Website</a> &middot; <a href="http://tracker.archiveteam.org/vidme/">Leaderboard</a></span></h2>
    """,
    utc_deadline = datetime.datetime(2017, 12, 15, 23, 59, 0)
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader,
        VERSION),
    PrepareDirectories(warc_prefix="vidme"),
	LimitConcurrent(NumberConfigValue(min=1, max=20, default="4",
        name="shared:wget_threads", title="Wget threads",
        description="The maximum number of concurrent downloads."),
		WgetDownload(
		    WgetArgs(),
		    max_tries=2,
		    accept_on_exit_code=[0, 4, 8],
		    env={
		        "item_dir": ItemValue("item_dir"),
		        "item_value": ItemValue("item_value"),
		        "item_type": ItemValue("item_type"),
		        "warc_file_base": ItemValue("warc_file_base"),
		    }
		),
    ),
    DeduplicateWarc(),
    PrepareStatsForTracker(
        defaults={"downloader": downloader, "version": VERSION},
        file_groups={
            "data": [
                ItemInterpolation("%(data_dir)s/%(warc_file_base)s.warc.gz")
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default="1",
        name="shared:rsync_threads", title="Rsync threads",
        description="The maximum number of concurrent uploads."),
        UploadWithTracker(
            "http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation("%(data_dir)s/%(warc_file_base)s_data.txt"),
                ItemInterpolation("%(data_dir)s/%(warc_file_base)s.warc.gz")
            ],
            rsync_target_source_path=ItemInterpolation("%(data_dir)s/"),
            rsync_extra_args=[
                "--recursive",
                "--partial",
                "--partial-dir", ".rsync-tmp",
            ]
        ),
    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)
