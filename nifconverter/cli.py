import fileinput
import multiprocessing
import re
import threading
import traceback
from sys import stdout
from time import time
from typing import Iterable
from urllib.parse import unquote

import click
import reactivex as reactivex
from reactivex import Observer
from reactivex.operators import do_action, map, observe_on, subscribe_on, buffer
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.scheduler.scheduler import Scheduler

from nifconverter.uriconverter import URIConverter
from nifconverter.utils import WIKIDATA_PREFIX

from nifconverter.index_sameas_uri_converter import IndexSameAsUriConverter

registered_converters = {cls.__name__: cls for cls in URIConverter.__subclasses__()}


def get_available_converters():
    return ', '.join(registered_converters.keys()) or '(none)'


@click.command()
@click.option('--converter', default='SameThingConverter',
              help='Conversion method, among the implemented ones: ' + get_available_converters())
@click.option('--target', default=WIKIDATA_PREFIX, help='The target namespace to convert the URIs to.')
@click.option('-i', '--infile', default='-', help='The source NIF file to read. If not provided, stdin is used.')
@click.option('-o', '--outfile', default='-', help='The target NIF file to write. If not provided, stdout is used.')
@click.option('--format', default='turtle', help='The RDF serialization format to use.')
def main(converter, target, infile, outfile, format):
    """
    Conversion utility for NIF files.

    This converts the identifiers used to annotate mentions in documents
    across knowledge bases. For instance, the following will convert
    a NIF file with DBpedia identifiers to a NIF file with Wikidata identifiers,
    using the default converter (which uses the DBpedia SameThing service):

       nifconverter -i dbpedia_nif.ttl -o wikidata_nif.ttl

    """
    converter_impl = registered_converters.get(converter)
    if converter_impl is None:
        raise click.BadParameter(
            'Invalid converter "{}". Supported converters are: {}'.format(converter, get_available_converters()))

    pattern = re.compile("([/:])taIdentRef <(.*?)>")
    converter = converter_impl(target_prefix=target)

    def replace_url_in_ta_ident_ref(line: str) -> str:
        # print("{}".format(threading.current_thread().name))
        return re.sub(pattern, lambda m: (
            "{}taIdentRef <{}>".format(m.group(1), converter.convert_one(unquote(m.group(2))) or m.group(2))
        ), line)

    thread_count = 4  # multiprocessing.cpu_count()
    pool_scheduler = ThreadPoolScheduler(thread_count)

    print("Using {} thread(s)".format(thread_count))

    stats = Stats()
    with fileinput.input(infile) as f, open(outfile, "w") if outfile != "-" else stdout as o:
        reactivex.from_iterable(GroupByEmptyLinesIterable(f)).pipe(
            observe_on(pool_scheduler),
            map(replace_url_in_ta_ident_ref),
            map(lambda line: o.write(line)),
            buffer(reactivex.interval(10)),
            do_action(on_next=lambda d: stats.add_and_print(len(d) if d else 1)),
        ).run()

    # translator = NIFTranslator(converter_impl(target_prefix=target))
    #
    # with click.open_file(infile) as f:
    #     nif = NIFCollection.loads(f.read())
    #
    # translator.translate_collection(nif)
    #
    # with click.open_file(outfile, 'w') as out:
    #     out.write(nif.dumps())


class GroupByEmptyLinesIterable(Iterable[str]):
    delegate: Iterable[str]
    acc: str = ''

    def __init__(self, it: Iterable[str]):
        self.delegate = it

    def __iter__(self):
        for line in self.delegate:
            if line.strip() == '':
                yield self.acc
                self.acc = ''
            else:
                self.acc += line


class Stats(object):
    count: int = 0
    start: float = time()

    def add(self, count: int):
        self.count += count

    def print(self):
        print('{:,} document(s) processed, {:,.2f} dps'.format(self.count, self.count / (time() - self.start)))

    def add_and_print(self, count: int):
        self.add(count)
        self.print()


if __name__ == '__main__':
    main()
