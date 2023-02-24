## Yeah1 dbt - guide for set up and running dbt
---
This is the dbt project for Yeah1 dbt models with BigQuery connection.
_dbt version: 1.3.0_

### Setting up Python environment

- Set up Python environment for developing
    ```bash
    # Create Python virtual environment
    python -m venv path/to/your/venv

    # Install dependencies
    source path/to/your/venv/bin/activate
    pip install -U pip setuptools wheel
    pip install -r requirements.txt
    ```

### Setting up gcloud authorization

- set default application login by
    ```bash
    gcloud auth application-default login
    ```

### dbt common operations

#### 1. Setting up

- Create your local dbt profiles.yml for development using the template `profiles-dev.yml`
- Refer: [Configure your profile](https://docs.getdbt.com/dbt-cli/configure-your-profile)

- Install dbt dependencies

    ```bash
    dbt deps
    ```

- Check connection

    ```bash
    dbt debug
    ```

#### 2. Generate dbt source, base model YAML files

- Create source YAML file

    ```bash
    dbt run-operation generate_source --args '{"name": "your_source_name", "schema_name": "your_schema_name"}'
    ```

- Create base models

    ```bash
    dbt run-operation generate_base_model --args '{"source_name": "your_source_name", "table_name": "your_table_name"}'
    ```

- Create model YAML file

    ```bash
    dbt run-operation generate_model_yaml --args '{"model_name": "model_name_to_create_yaml"}'
    ```

- Refer: [dbt-codegen](https://github.com/dbt-labs/dbt-codegen#generate_model_yaml-source)

#### 3. Run models

- Run normal models

    ```bash
    dbt run -m model_name_or_tag_or_state
    ```
    
- Run a group of normal models (without running whole pipeline)

    ```bash
    dbt run -s +model_name_or_tag_or_state # graph operator = '+' in front --> this will run all upstream model of this and the model itself
    ```

    ```bash
    dbt run -s model_name_or_tag_or_state+ # graph operator = '+' at tail -->this will run all downstream model of this and the model itself
    ```

- Run snapshot models

    ```bash
    dbt snapshot -s snapshot_name
    ```

- References (read more under each section for in-depth command guides)
    - [Learn dbt commands](https://docs.getdbt.com/reference/dbt-commands)
    - [Syntax overview](https://docs.getdbt.com/reference/node-selection/syntax)
    - [Graph operators](https://docs.getdbt.com/reference/node-selection/graph-operators)

#### 4. Run snapshot

- Run dbt snapshots

    ```bash
    dbt snapshot
    ```
- Reset snapshot, go to BQ and delete the snapshot table

- Refer: 
    - [dbt documentation on snapshots](https://docs.getdbt.com/docs/build/snapshots)
    - [list of snapshotted tables](https://docs.google.com/spreadsheets/d/1KLdv0yur-evDPUZ7baPBHuZfBY2EgW5pIUcR2JbwL-4/edit?usp=sharing)

#### 5. Test models

- Run dbt tests

    ```bash
    dbt test -m model_name_or_tag_or_state
    ```

- Check required tests (Refer: [dbt-meta-testing](https://hub.getdbt.com/tnightengale/dbt_meta_testing/latest/))

    ```bash
    dbt run-operation required_tests
    ```

### Linting

- Lint code with

    ```bash
    sqlfluff lint path/to/file/or/folder
    ```

- Auto fix code

    ```bash
    sqlfluff fix path/to/file/or/folder
    ```

- Refer: [sqlfluff](https://github.com/sqlfluff/sqlfluff)

### Pre-commit hook

- Install pre-commit hooks

    ```bash
    pre-commit install
    ```

- Uninstall pre-commit hooks

    ```bash
    pre-commit uninstall
    ```

- Refer: [pre-commit](https://pre-commit.com/)

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Project conventions

Following definitions are important for maintaining consistent naming and judgement of a model's purpose in this project.

#### docs

Docs are stored under `doc` folder and will store following:

- Table descriptions
- Column name descriptions
- Common columns/ tables descriptions

Convention for naming `doc.md` file: [sourcesystem]_columns/tables.md
Must maintain this documentation for dbt `persist_doc` function to work & expose the information on BigQuery.

#### source

- Naming convention: `[business_unit]__sourcesystem__[all/ a single db name].yml`
- Definition: sources must always be defined in `yml` file under models/source folder
- Transformations required/ allowed: none
- Tests required: test can be defined at source per dbt, however as base models are created 1:1 with source, we skip test at this level.

#### base

- Naming convention: `base__[source_name]__[model_name]`
- Definition: base models are materialized as `view` by default and must always be created in 1:1 relation with RAW tables (data sources)
- Transformations required/ allowed:
  - data type conversion (datetime, json flatten, etc.)
  - filtering out deleted records
  - renaming of source columns to ensure consistency, clarity, and avoid reserved naming
  - No joins should exist at base models
- Tests required: must-have tests on PK of model `['unique', 'not_null']`

With main concept of DRY (don't repeat yourself) applied as a dbt-best-practice, `staging` and `intermediate` models are often required before you can build a `mart` model.

#### staging

- Naming convention: `stg__[source_name]__[model_name]`
- Definition: models that join base models from a single schema source, aims to **denormalize** and group columns in a logical way that will be **utilized in downstream more than once**.
- Example: joining together `material` and `material_type` table via `material_type_id` that exist in both tables, to create a staging table that stores all attributes of material in a model called `stg__case__materials`
- Transformations required/ allowed:
  - **joins** of 2 or more models to denormalize how data is stored before utilizing them in downstream
  - optional: `case when` or similar enrichment transformations that can be derived from such joins and make sense to be stored at staging and not intermediate model (refer below)
- Tests required: must-have tests on PK of model `['unique', 'not_null', 'dbt_utils.at_least_one]`

#### intermediate

- Naming convention:
  - `int__[x_by_y]__agg` for models that aggregate entity `x` by entity `y` (example: `customer` is an entity, so is `order`, each have its own `customer_id` and `order_id` at base)
  - `int__[A_B_x]__unioned` for models that *union* 2 sources: `A` and `B` and store `x` as the primary key
  - `int__[A_B_x]__joined` for models that *join* 2 sources: `A` and `B` and store `x` as the primary key
  - if anything deviates from these scenarios, it's important team discuss and draw out a shared convention update
  - When no source is mentioned in the naming, we should automatically assume they come from single source schema
- Definition: intermediate models are downstream of staging models, OR of base where applicable, it will **combine data from across schemas** and/or store **business logic transformations** at this layer.
- Transformations required/ allowed: intermediate models usually have to union or join 2 or more tables together to achieve mentioned purpose, with business logic transformations.

#### mart

- Naming convention: `fct_[model_name]` and `dim_[model_name]` per dbt convention
- Definition: `fct` is fact model, `dim` is dimension model. We apply dimensional modeling concepts. Mart models can be further organized into separate sub-folders which align with **departmental** usage

#### snapshot

- Naming convention: `snapshot__[source_dataset_name]__[table_name]`

#### Deciding between staging and intermediate and mart

If above definitions and transformations guides are not enough to help you decide, here are some more considerations:

- Build everything at final layer first (i.e `mart`), then move pieces of logic back to `stg` and `int` where necessary - this is usually clearer with time and requires occasional refactoring
- When you develop, remember to always document the model description in relevant `yml` file, so that its purpose is well understood by other developers and promote its reusability, as well as relevance, with time.
- In deciding between staging and intermediate, remember *DRY concept* and that usually we want to store a common transformation logic *as upstream as possible*, without going against the project conventions.


### Known issues and fix

#### External table in BQ

when running dbt that has a model that reference an external table (from Google sheet, GCS,....) and you encounter this error message

```
    Access Denied: BigQuery BigQuery: Permission denied while getting Drive credentials.
```

Run this in your terminal and follow the link to login for gcloud SDK like normal to authorize application login to google drive

```
    gcloud auth application-default login --scopes=openid,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/sqlservice.login,https://www.googleapis.com/auth/drive
```

_Reference: https://docs.getdbt.com/faqs/troubleshooting/access-gdrive-credential_
