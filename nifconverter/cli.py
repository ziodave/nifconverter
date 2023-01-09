import fileinput
import re
import traceback
from sys import stdout
from urllib.parse import unquote

import click
import reactivex as reactivex
from reactivex.operators import map
from pynif import NIFCollection

from nifconverter.translator import NIFTranslator
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
        return re.sub(pattern, lambda m: (
            "{}taIdentRef <{}>".format(m.group(1), converter.convert_one(unquote(m.group(2))) or m.group(2))
        ), line)

    with fileinput.input(infile) as f, open(outfile, "w") if outfile != "-" else stdout as o:
        reactivex.from_iterable(f).pipe(
            map(replace_url_in_ta_ident_ref),
            map(lambda line: o.write(line))
        ).subscribe(on_error=lambda e: traceback.print_exc())

    # translator = NIFTranslator(converter_impl(target_prefix=target))
    #
    # with click.open_file(infile) as f:
    #     nif = NIFCollection.loads(f.read())
    #
    # translator.translate_collection(nif)
    #
    # with click.open_file(outfile, 'w') as out:
    #     out.write(nif.dumps())


if __name__ == '__main__':
    main()
