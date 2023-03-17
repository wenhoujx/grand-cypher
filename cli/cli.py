
from pkg_resources import require
import click
from click import STRING

@click.group()
def cli():
    pass

@click.command()
@click.option('-s', '--schema', help="schema file", required=True,  type=click.Path(exists=True))
@click.option('--cypher-file', help='cyper script file', required=True, type=click.Path(exists=True))
def run(schema, cypher_file):
    click.echo(f"schema {schema}")
    click.echo(f"cypher {cypher_file}")
    

cli.add_command(run)

if __name__ == '__main__':
    cli()
