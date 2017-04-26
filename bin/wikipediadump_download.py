#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Ported to python 3.4 by Andrea Esuli (andrea@esuli.it), modified to download the "lastest" dump,
# plus options to download only a selection of languages, removed the dependency from wget.

# Copyright (C) 2011-2014 WikiTeam
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import os
import re
import sys
import time
import codecs
import urllib
import urllib.request
import bz2
import shutil
from contextlib import closing


def main():
    sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer)
    parser = argparse.ArgumentParser(
        description='Downloader of Wikimedia dumps')
    parser.add_argument('-f', '--force', help='Force download even if already successfully downloaded', required=False,
                        action='store_true')
    parser.add_argument('-n5', '--nomd5', help='Disable md5 check', required=False, action='store_true')
    parser.add_argument('-nz', '--nobz2', help='Disable bz2 integrity check', required=False, action='store_true')
    parser.add_argument(
        '-r', '--maxretries', help='Max retries to download a dump when md5sum doesn\'t fit. Default: 3',
        required=False)
    parser.add_argument(
        '-t', '--timeout', help='Timeout in second for http connections. Default: 180',
        required=False)
    parser.add_argument(
        '-o', '--out', help='Output base path', required=True)
    parser.add_argument(
        '-l', '--lang', help='Relevant languages file', required=False)
    parser.add_argument(
        '-m', '--mirror', help='Mirror from which to download', required=False)
    parser.add_argument(
        '-s', '--single',
        help='Prefer single file over multiple files in big wikis (default: multiple)',
        required=False, action='store_true')
    args = parser.parse_args()

    langdict = {}
    with open(args.lang, encoding='utf8') as langfile:
        for line in langfile:
            fields = line.strip().split('\t')
            langdict[fields[1]] = fields[0]
    missinglangs = {}

    basepath = args.out

    maxretries = 3
    if args.maxretries and int(args.maxretries) >= 0:
        maxretries = int(args.maxretries)
    timeout = 180
    if args.timeout and int(args.timeout) >= 0:
        timeout = int(args.timeout)

    dumpsdomain = 'http://dumps.wikimedia.org'
    if args.mirror:
        dumpsdomain = args.mirror

    if len(langdict.keys()) == 0:
        with closing(urllib.request.urlopen('%s/backup-index.html' % (dumpsdomain)), timeout=timeout) as f:
            raw = f.read()

        m = re.compile(
            r'<a href="(?P<project>[^>]+wiki)/(?P<date>\d+)">[^<]+</a>: <span class=\'done\'>Dump complete</span>').finditer(
            raw.decode())
        for i in m:
            wiki = i.group('project')
            langdict[wiki] = 'Added from web'

    for key in sorted(langdict.keys()):

        missinglangs[key] = ['no file found']

        print(langdict[key], ' ', key, flush=True)
        try:
            with urllib.request.urlopen('%s/%swiki/latest/' % (dumpsdomain, key), timeout=timeout) as f:
                htmlproj = f.read()
        except:
            print('Error on %s %s' % (langdict[key], key))
            continue

        path = os.sep.join((basepath, key))
        if not os.path.exists(path):
            os.makedirs(path)

        singlere = re.compile(r'<a href="(?P<urldump>%swiki-latest-%s)">' %
                       (key,'pages-articles\.xml\.bz2'))
        splitre = re.compile(r'<a href="(?P<urldump>%swiki-latest-%s)">' %
                       (key,'pages-articles\d+\.xml[^\.]*\.bz2'))
        if args.single:
            m =  singlere.findall(htmlproj.decode())
        else:
            m = splitre.findall(htmlproj.decode())
            if len(m)==0:
                m =  singlere.findall(htmlproj.decode())

        urldumps = []
        for i in m:
            urldumps.append(
                '%s/%swiki/latest/%s' % (dumpsdomain, key, i))#.group('urldump')))

        urldumps = list(set(urldumps))

        if len(urldumps)>1 and args.single:
            for u in urldumps:
                if re.match(r'pages-articles\d*\.xml\.bz2',u):
                    urldumps.remove(u)
                    break

        missinglangs[key] = urldumps

        for urldump in sorted(urldumps):
            time.sleep(1) #ctrl+c
            dumpfilename = urldump.split('/')[-1]
            dumpfilename = re.sub(r'^(.*)-p[0-9]+p[0-9]+(\.bz2)', r'\1\2', dumpfilename)
            print('Filename is %s' % dumpfilename)
            fulldumpfilename = os.sep.join((path, dumpfilename))
            if not os.path.exists(fulldumpfilename) or args.force:
                try:
                    os.remove(fulldumpfilename)
                except:
                    pass
                try:
                    os.remove(fulldumpfilename+'.tmp')
                except:
                    pass
                corrupted = True
                maxretries2 = maxretries
                while corrupted and maxretries2 > 0:
                    maxretries2 -= 1

                    try:
                        with closing(urllib.request.urlopen(urldump, timeout=timeout)) as u, open(fulldumpfilename+'.tmp',
                                                                                                  'wb') as f:
                            meta = dict(u.info())
                            size = int(meta["Content-Length"])
                            print("%s %s" % (urldump, size))
                            downloaded = 0
                            blocksize = 64 * 1024
                            while True:
                                buffer = u.read(blocksize)
                                if not buffer:
                                    break

                                downloaded += len(buffer)
                                f.write(buffer)
                                print("%10d  [%3.2f%%]" % (downloaded, ((downloaded * 100.) / size)), end='\r',
                                      flush=True)
                    except:
                        ret = 1
                    finally:
                        print(flush=True)
                        if os.path.exists(fulldumpfilename+'.tmp'):
                            if os.stat(fulldumpfilename+'.tmp').st_size == size:
                                print('%s download size OK' % key)
                                ret = 0
                            else:
                                print('%s wrong download size' % key)
                                ret = 1
                        else:
                            print('%s file not downloaded' % key)
                            ret = 1

                    if ret == 0:
                        if not args.nomd5:
                            # md5check may not work on 'lastest' dumps because the latest md5 file
                            # may not be synchronized with latest dumps
                            os.system('md5sum %s > md5' % (fulldumpfilename+'.tmp'))
                            f = open('md5', 'r')
                            raw = f.read()
                            f.close()
                            md51 = re.findall(
                                r'^(?P<md5>[a-f0-9]{32})', raw)[0]

                            with closing(urllib.request.urlopen(
                                            '%s/%swiki/latest/%swiki-latest-md5sums.txt' % (
                                            dumpsdomain, key, key), timeout=timeout)) as f:
                                raw = f.read().decode()

                            f = open('%s/%s-latest-md5sums.txt' %
                                     (path, key), 'w', encoding='utf8')
                            f.write(raw)
                            f.close()

                            dumpmatch = re.sub('latest', '[0-9]{8}', dumpfilename)

                            md52 = re.findall(
                                r'(?P<md5>[a-f0-9]{32})\s+%s' % dumpmatch, raw)[0]

                            if md51 == md52:
                                print('%s tested OK (md5)' % key)
                                corrupted = False
                                shutil.move(fulldumpfilename + ".tmp", fulldumpfilename)
                            else:
                                print('%s wrong md5' % key)
                                try:
                                    os.remove(fulldumpfilename+'.tmp')
                                except:
                                    pass
                        elif not args.nobz2:
                            # without md5 the only other way to check the file is to expand it
                            # it costs some time but i see no other option to check it
                            try:
                                with bz2.BZ2File(fulldumpfilename+'.tmp') as zfile:
                                    while True:
                                        if not zfile.readline():
                                            break
                            except (IOError, EOFError):
                                print('%s bz2 corrupted' % key)
                                try:
                                    os.remove(fulldumpfilename+'.tmp')
                                except:
                                    pass
                            else:
                                print('%s tested OK (bz2)' % key)
                                shutil.move(fulldumpfilename + ".tmp", fulldumpfilename)
                                corrupted = False
                        else:
                            print('%s assuming OK (no md5 or bz2 check)' % key)
                            shutil.move(fulldumpfilename + ".tmp", fulldumpfilename)
                            corrupted = False

                        if not corrupted and urldump in missinglangs[key]:
                            missinglangs[key].remove(urldump)
                    else:
                        try:
                            os.remove(fulldumpfilename+'.tmp')
                        except:
                            pass
            else:
                print('Already downloaded.')
                missinglangs[key].remove(urldump)

    with open(args.lang + ".download_missing.txt", mode='w', encoding='utf8') as missingfile:
        for l in missinglangs:
            for u in missinglangs[l]:
                print('%s\t%s\t%s' % (langdict[l], l, u), file=missingfile)


if __name__ == '__main__':
    main()
