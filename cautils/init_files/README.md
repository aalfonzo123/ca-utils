This directory represents a [conversational analytics data agent](https://docs.cloud.google.com/gemini/docs/conversational-analytics-api/reference/rest/v1alpha/projects.locations.dataAgents/create?rep_location=global), split into multiple, easier to handle, files.

## autogen.yaml

This file serves as input to the `ca-utils data-agent autogen` command. 
In this file you specify the BigQuery tables from which metadata will be extracted.

## datasourceReferences.yaml

This file has metadata on all the tables the data agent will use for its queries. 
Create it with `ca-utils data-agent autogen`, it does not make much
sense to create it manually.

Note: make sure that all fields and all tables have a description. It makes a __very important__ difference in the agent’s ability to create proper SQL and therefore a __very important__ difference in response quality. To automatically create those descriptions, you can use [Gemini BQ data insights](https://docs.cloud.google.com/bigquery/docs/data-insights#add_a_table_description).

## exampleQueries.yaml

This file has example prompts and their expected SQL queries. It is optional, but recommended
if the agent is not able to respond to particular prompts.

This file should be created __manually__. The `ca-utils data-agent autogen` command can create a sample one, but it won't have any real-life useful queries.

## glossaryTerms.yaml

This file has terms that are particular to your business domain, and that conversational analytics
would not know about unless you added them. These are used when translating prompts into SQL.

This file must be created __manually__. The `ca-utils data-agent autogen` command does not generate this file, as it does not know
your particular business domain or custom terms.

## schemaRelationships.yaml

This file has a list of relationships between tables. It is used by the data agent to do joins
between them on SQL statements.

A draft of this file should be created with `ca-utils data-agent autogen`, and then manually
modified, as it can have mistakes or omissions. For this file, autogen infers contents
using an LLM, it is not a deterministic process.

## systemInstruction.yaml

This file has the system instructions for the agent.

The `ca-utils data-agent autogen` command does not generate this file. There is a default
version of this file that you should customize.

Note that this file is just text (no arrays, objects, etc.) but is still managed as a yaml
for these reasons:
- Consistency with the other files
- Ability to add comments. These will be ignored when uploading to the c.a. api
