__author__ = 'Andrea Esuli'

# Wrapper to  Attardi's TANL script that extracts simple text out of
# wikipedia XML dumps.
# Copyright 2015 Istituto di Scienza e Tecnologie dell'Informazione ``A. Faedo''
#
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
import glob
import re
import shutil
import codecs
import sys
import subprocess
import threading
from contextlib import closing
import bz2

dryrun = False

def main():
    sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer)
    parser = argparse.ArgumentParser('Bulk converter of wikipedia dumps to text')
    parser.add_argument(
        '-i', '--input', help='Input base path', required=True)
    parser.add_argument(
        '-o', '--output', help='Output base path', required=True)
    parser.add_argument(
        '-d', '--dryrun', help='Dry run, do not produce output', required=False, action='store_true')
    parser.add_argument(
        '-l', '--lang', help='Relevant languages file', required=False)
    args = parser.parse_args()
    global dryrun
    dryrun = args.dryrun
    langdict = {}
    if hasattr(args, 'lang'):
        with open(args.lang, encoding='utf8') as langfile:
            for line in langfile:
                fields = line.strip().split('\t')
                print(fields[1])
                langdict[fields[1]] = (fields[0], False)
    sys.stdout.flush()
    filedict = processpath(args.input, langdict)
    converttotext(filedict, args.output)
    with open(args.lang + ".totext_missing.txt", mode='w', encoding='utf8') as missingfile:
        for key, value in sorted(langdict.items()):
            if value[1] is False:
                print('%s\t%s' % (value[0], key))
                print('%s\t%s' % (value[0], key), file=missingfile)


def processpath(path, langdict, filedict=None):
    if filedict is None:
        filedict = {}
    if os.path.isfile(path):
        processfile(path, langdict, filedict)
    elif os.path.isdir(path):
        for subpath in glob.glob(path + os.sep + '*'):
            processpath(subpath, langdict, filedict)
    return filedict


def processfile(filename, langdict, filedict):
    resep = '/'
    if os.sep == '\\':
        resep = r'\\'
    found = re.findall(r'%s(([a-z]+)wiki-.+).bz2' % resep, filename)
    if len(found) == 1:
        wiki = found[0][1]
        filepref = found[0][0]
        if not wiki in langdict:
            return
        langdict[wiki] = (langdict[wiki][0], True)
        list = filedict.get(wiki, [])
        list.append([filename, filepref])
        filedict[wiki] = list


def pump(filename, pipe):
    """Decompress *filename* and write it to *pipe*."""
    with closing(pipe), bz2.BZ2File(filename) as input_file:
         shutil.copyfileobj(input_file, pipe)


def converttotext(filedict, out):
    for wiki, filepairs in sorted(filedict.items()):
        if dryrun:
            for filename, filepref in filepairs:
                print(wiki, filename)
        else:
            for filename, filepref in filepairs:
                go = False
                path = os.sep.join([out, wiki, filepref])
                if not os.path.exists(path):
                    go = True
                else:
                    if os.path.getmtime(filename) > os.path.getmtime(path):
                        shutil.rmtree(path, ignore_errors=True)
                        go = True
                        print('Wiki %s outdated' % wiki)
                        break
                if go:
                    shutil.rmtree(path + '.tmp', ignore_errors=True)

                    p = subprocess.Popen(['python', 'WikiExtractor.py', '-o', path + ".tmp"], stdin=subprocess.PIPE, stdout=sys.stdout, bufsize=-1)
                    threading.Thread(target=pump, args=[filename, p.stdin]).start()
                    p.wait()
                    shutil.move(path + ".tmp", path)
                else:
                    print('skipping %s' % wiki)


if __name__ == '__main__':
    main()