Spatial_disaggregation
==============================

A tool to spatially disaggregate EU's country level decarbonisation pathways to NUTS3 regions 

Project Organization
------------

    ├── README.md          <- The top-level README for developers using this project.
    ├── data               <- **The data is not commited. It is available in the PIK cloud**
    │   ├── input          <- Data from different sources.
    │   │   ├── raw          <- as-is data.
    │   │   ├── processed    <- processed data from raw data files.
    │   ├── output         <- Disaggregated data. 
    │
    ├── api                <- Django API design 
    │
    ├── notebooks          <- Jupyter notebooks - Exploratory, report prep and data processing
    │
    ├── zoomin             <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to query and process data 
    │   │
    │   ├── db_utils       <- helper functions to create, delete, and connect to a database 
    │   │
    │   ├── main_modules   <- Scripts to disaggregate data 
    │   │   
    │   └── visualization  <- (from template)  Scripts to create exploratory and results oriented visualizations
    │
    ├── reports            <- Generated PDF and LaTeX reports 
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so zoomin can be imported
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │ 
    ├── requirements.yml  <- The requirements file in yaml format. Produced manually. This is currently used in 
    │                         deployment
    │
    ├── references         <- (from template) Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── docs               <- (from template) A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- (from template) Trained and serialized models, model predictions, or model summaries
    │
    ├── LICENSE
    │
    ├── Makefile           <- (from template) Makefile with commands like `make data` or `make train`f
    │
    └── tox.ini            <- (from template)  tox file with settings for running tox; see tox.readthedocs.io
    

--------

Installation steps 
------------

0. Before you begin, please make sure you have mamba installed in your base environment
    ```bash
    conda install mamba -c conda-forge
    ```

1. Clone this repository:
    ```bash
    git clone https://jugit.fz-juelich.de/iek-3/shared-code/localised/spatial_disaggregation.git
    ```

2. Install dependencies and the repo in a clean conda environment:
    ```bash
    cd spatial_disaggregation
    mamba env update -n zoomin_dev_env --file=requirements.yml
    conda activate zoomin_dev_env
    ```

3. Additionally, for geospatial data anaylysis, geokit is used. Please install this:
    ```bash
    cd ..
    git clone git@jugit.fz-juelich.de:iek-3/shared-code/geokit.git
    cd geokit
    pip install -e .
    ```

4. The building data comes from an in-house API client - builda. Please install this:
    ```bash
    cd ..
    git clone git@jugit.fz-juelich.de:iek-3/groups/urbanmodels/personal/dabrock/builda-client.git --branch v1.0
    cd builda-client
    pip install -e .
    ```
