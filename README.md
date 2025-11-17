# ca-utils

## Introduction

This is a CLI utility to:
* List, create, etc. Conversational Analytics agents.
* Export BigQuery metadata in the format that Conversational Analytics expects

It is mainly a wrapper around REST methods for those functionalities, using these libraries:
- [Requests](https://requests.readthedocs.io/en/latest/) to execute http methods
- [Cyclopts](https://github.com/BrianPugh/cyclopts) to easily convert Python functions into CLI commands
- [Rich](https://github.com/Textualize/rich), to list items in table format, instead of a raw JSON dump

## How to use

For regular use, execute this "uv tool install":

`uv tool install git+https://github.com/aalfonzo123/ca-utils`

For occasional use, uvx works:

`uvx --from git+https://github.com/aalfonzo123/ca-utils ca-utils`

See also: [uv tools documentation](https://docs.astral.sh/uv/guides/tools/#requesting-different-sources)
## Features

### Auto-generation of needed files (see below)

To auto-generate, write a simple file with the data sources you will use. I.e.

bqDataSources:
  - my-project-id.fcc_political_ads.*
  - my-project-id.fda_food.food_enforcement

The asterisk means "all tables in the dataset"

### Auto-generation of tableReferences

Conversational Analytics Agents use a tableReferences object that describes tables.
This tool generates it from metadata, and infers missing field descriptions using LLM
i.e.

```
bq:
  tableReferences:
  - datasetId: fcc_political_ads
    projectId: my-project-id
    schema:
      fields:
      - description: "Unique identifier for the station."
        mode: NULLABLE
        name: stationId
        type: STRING
```


### Auto-generation of schemaRelationship

Conversational Analytics Agents use a SchemaRelationship object that describes foreign
key relationships among tables, used for joins.
This tool infers the relationships from metadata using an LLM.
i.e.

```
- confidenceScore: 10
  leftSchemaPaths:
    paths:
    - stationId
    tableFqn: bigquery.googleapis.com/projects/my-project-id/datasets/fcc_political_ads/tables/broadcast_tv_radio_station
  rightSchemaPaths:
    paths:
    - stationId
    tableFqn: bigquery.googleapis.com/projects/my-project-id/datasets/fcc_political_ads/tables/file_history
  sources:
  - LLM_SUGGESTED
```


### Upload and download data agent definitions

Given a directory like this one:
agent1
- datasourceReferences.yaml
- systemInstruction.txt
- datasourceReferences.yaml

You can tell the tool to upload "agent1", and it will create (or update) an agent with the specifications on the files.

Likewise, if you have an agent on the server called " agent2" you can download it, and a directory with the same files shown will be created.

### List existing agents

They will be shown in an easy to read "rich" table

### Chat with data agents

A single prompt can be sent to a data agent, the response contains every step of the reasoning, including the SQL statement and final result.
