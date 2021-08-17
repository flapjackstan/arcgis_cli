## I-Team Python GIS Wrapper

Command line shell for interacting with the Arcgis API 

### Quick start

To quickly see a list of commands on the program use the --help or -h flag 

`python gis-cli.py --help`

### Suggested Workflow

1. Geocode
2. Upload
3. Enrich
4. Download
5. Breakdown

### Limitations

- Geocode command can only handle 1000 addresses in a single usage
- Cannot publish detailed properties (e.g. description, tags, sharing, etc..)
- Cannot publish styling properties (e.g. map colors, segments, etc..)
- Publish fails if there is already a feature layer named the same 
