import os
import sys

import click

import kachery_client as kc


@click.group(help="Kachery peer-to-peer command-line client")
def cli():
    pass

@click.command(help="Load or download a file.")
@click.argument('uri')
@click.option('--dest', default=None, help='Optional local path of destination file.')
def load_file(uri, dest):
    x = kc.load_file(uri, dest=dest)
    print(x)

@click.command(help="Store a file on the local node.")
@click.argument('path')
def store_file(path: str):
    x = kc.store_file(path)
    print(x)

@click.command(help="Store a link to file locally.")
@click.argument('path')
def link_file(path: str):
    x = kc.link_file(path)
    print(x)

@click.command(help="Download a file and write the content to stdout.")
@click.argument('uri')
@click.option('--start', help='The start byte (optional)', default=None)
@click.option('--end', help='The end byte non-inclusive (optional)', default=None)
def cat_file(uri, start, end):
    old_stdout = sys.stdout
    sys.stdout = None

    if start is None and end is None:
        path1 = kc.load_file(uri)
        if not path1:
            raise Exception('Error loading file for cat.')
        sys.stdout = old_stdout
        with open(path1, 'rb') as f:
            while True:
                data = os.read(f.fileno(), 4096)
                if len(data) == 0:
                    break
                os.write(sys.stdout.fileno(), data)
    else:
        assert start is not None and end is not None
        start = int(start)
        end = int(end)
        assert start <= end
        if start == end:
            return
        sys.stdout = old_stdout
        kc.load_bytes(uri=uri, start=start, end=end, write_to_stdout=True)

@click.command(help="Display kachery_client version and exit.")
def version():
    click.echo(f"This is kachery_client version {kc.__version__}")
    exit()

cli.add_command(cat_file)
cli.add_command(load_file)
cli.add_command(store_file)
cli.add_command(link_file)
cli.add_command(version)
