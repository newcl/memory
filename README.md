# What is Memory?

A tool to manage all my photo/video/.etc.

- Use python as language, if necessary use golang/rust to implement critical component and expose to memory, for now lets just use python for everything.
- Use sqlite as metadata storage for easy query and self sufficient management

# Usage 

``` 
- use current folder as home folder
- create .memory folder to manage all our stuff 
- if .memory exists abort and do nothing
```
memory init 

```
- scan source folder to detect all known media files and copy to current folder 
- check for duplicates against all files before actually copying file over 
- if no idential file found
  - copy file into current folder for now, lets just keep everything in the folder, if file name conflicts add suffix e.g. readable timestamp
  - update metadata to include this file e.g. file hash, metadata
- when no new files found, no changes should be made
```
memory import source_folder 

```
- List all files that have not been uploaded only
```
memory upload --dryrun

```
Upload all new files to cloud storage
```
memory upload s3/gcloud/azure