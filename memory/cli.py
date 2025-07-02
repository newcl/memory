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
@click.option('--recursive/--no-recursive', default=True, show_default=True, help='Recursively import files from subfolders (default: true)')
@click.option('--threads', default=2, show_default=True, type=int, help='Number of threads to use for import (default: 2)')
def import_cmd(source_folder, recursive, threads):
    """
    Imports media files from SOURCE_FOLDER into the Memory home folder.
    Only new files are copied and added to the database.
    """
    core.import_folder(source_folder, recursive=recursive, threads=threads)

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
def destroy():
    """
    Deletes the .memory folder and its contents, undoing 'memory init'.
    """
    core.delete_memory()

@cli.command()
@click.argument('record_id')
def delete(record_id):
    """
    Deletes a single file from the database and disk by its record id (file_hash).
    """
    core.delete_file_by_id(record_id)

@cli.command()
@click.option('--no-metadata', is_flag=True, help='Show stats for all files regardless of metadata extraction status.')
def stats(no_metadata):
    """
    Show statistics: total files, total size, metadata extraction rate, upload status.
    """
    core.print_stats(no_metadata=no_metadata)

@cli.command()
@click.option('--samesize', is_flag=True, help='List groups of files with the same file size (more than one per group).')
@click.option('--videos', is_flag=True, help='Restrict detection to known video formats.')
@click.option('--photos', is_flag=True, help='Restrict detection to known photo formats.')
@click.option('--visual', is_flag=True, help='Detect visually similar files (photos/videos) using perceptual hashing.')
@click.option('--populate-hash', is_flag=True, help='Populate missing perceptual hashes for all files in the database.')
def detect(samesize, videos, photos, visual, populate_hash):
    """
    Detect potential duplicates or issues in the managed files.
    """
    if populate_hash:
        core.populate_perceptual_hashes()
    elif visual:
        core.detect_visual(videos=videos, photos=photos)
    elif samesize:
        core.detect_samesize(videos=videos, photos=photos)
    else:
        print('No detection mode specified. Use --samesize, --visual, or --populate-hash.')

@cli.command()
def migrate():
    """
    Check for and add any missing columns to the files table in the database.
    """
    core.migrate_files_table()

@cli.command()
@click.argument('folder', type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True))
def scan(folder):
    """
    Scan the specified folder for files not under management and print stats by total number, total size, and by extension.
    """
    core.scan_unmanaged_files(folder)

if __name__ == '__main__':
    cli()
