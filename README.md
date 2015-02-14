===============
Wikipedia tools
===============

Set of tools to process wikipedia dumps:
 - port to python 3.4 and improvement of the [WikiTeam download tool](https://github.com/WikiTeam/wikiteam) to download the latest dumps.
 - wrapper to convert them to plain text using [Attardi's TANL script](https://github.com/aesuli/wikipedia-extractor).

Wikipedia downloader
--------------------

Example of command:

<pre>python wikipediadump_download.py -s -nz -n5 -m http://dumps.wikimedia.your.org/ -o f:\wikipedia -l f:\wikipedia.languages.txt</pre>

This command downloads the dumps from the your.org mirror, only checking the size of the downloaded file.
