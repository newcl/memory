import click
from memory import core

@click.group()
def cli():
    """A tool to manage your photo/video/etc. collection."""
    pass

@cli.command()
def init():
    """
    Initializes Memory in the current folder.
    Scans for media files and sets up the .memory database.
    """
    core.init_memory()

@cli.command('import') # Use 'import' as the command name, but func remains import_cmd
@click.argument('source_folder', type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True))
def import_cmd(source_folder):
    """
    Imports media files from SOURCE_FOLDER into the Memory home folder.
    Only new files are copied and added to the database.
    """
    core.import_folder(source_folder)

@cli.command()
@click.option('--dryrun', is_flag=True, help="List files that would be uploaded without performing the upload.")
@click.argument('cloud_target', required=False, type=click.Choice(['s3', 'gcloud', 'azure']))
def upload(dryrun, cloud_target):
    """
    Uploads new files to cloud storage.
    Specify 's3', 'gcloud', or 'azure' as the target.
    """
    if dryrun:
        if cloud_target:
            click.echo(f"Warning: --dryrun option ignores the cloud_target '{cloud_target}'. Listing all unuploaded files.")
        core.upload_dry_run()
    elif cloud_target:
        core.upload_to_cloud(cloud_target)
    else:
        click.echo("Error: Please specify a cloud target (s3, gcloud, azure) or use --dryrun.")
        click.echo("Usage: memory upload [--dryrun] <cloud_target>")

@cli.command()
def delete():
    """
    Deletes the .memory folder and its contents, undoing 'memory init'.
    """
    core.delete_memory()

if __name__ == '__main__':
    cli()
