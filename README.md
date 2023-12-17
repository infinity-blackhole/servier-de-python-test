# Python and SQL Data Engineering Project

## Objective

This repository addresses two distinct data engineering exercises: Python Data
Engineering and SQL. The goal is to develop high-quality code and data pipelines
to process and analyze provided datasets.

### Methodology

The project follows an exploration, experimentation, and implementation
approach. The methodology involves initially using Jupyter notebooks to
experiment with and understand the required behavior. Subsequently,
production-grade libraries and frameworks are employed to implement solutions at
scale.

In the case of the Python Data Engineering exercise (Exercise 1), Apache Beam
Interactive was initially used to pull the entire dataset into a Pandas
DataFrame for experimentation. The real implementation, however, leveraged
Apache Beam without pulling data from source to sink, ensuring efficiency and
scalability in a production environment.

The structure of the repository is designed to provide clarity and organization,
separating data, notebooks, and source code for both exercises. This facilitates
ease of understanding, collaboration, and further development.

Feel free to explore the notebooks for detailed insights into the exploratory
and experimental phases of the project. The source code in the `src` directory
reflects the final, scalable implementation of the data pipelines.

### Repository Structure

The repository adheres to the conventions of a standard PDM-based structure,
leveraging a modern `pyproject.toml` manifest. PDM, is a Python package manager
designed to address some limitations of the traditional `setup.py` approach.

### Why PDM?

1. **Improved Dependency Management:**
   - PDM provides a more robust and efficient approach to managing Python
     dependencies, ensuring consistent and reproductible environments across
     different systems via a lockfile `pdm.lock` and `pyproject.toml` manifest.

2. **Performance and Speed:**
   - PDM is built for improved performance, offering faster dependency
     resolution and installation processes compared to traditional tools.

3. **Enhanced Dependency Resolution Algorithm:**
   - PDM utilizes a more sophisticated dependency resolution algorithm,
     minimizing conflicts and providing a reliable environment for project
     development.

### Leveraging Apache Beam in the Python Exercise

Apache Beam is selected for the Python exercise to address the specific needs of
scalable and parallelizable data processing:

1. **Portable and Cross-Platform:**
   - Apache Beam is designed to be portable across different execution engines.
     This ensures compatibility with various environments, making it easier to
     adapt to different production settings.

2. **Integration with Data Processing Ecosystems:**
   - Apache Beam seamlessly integrates with popular data processing ecosystems,
     such as Apache Spark and Google Cloud Dataflow. This integration capability
     enhances interoperability with existing data infrastructure.

### Utilizing DuckDB in the SQL Exercise

DuckDB is chosen for the SQL exercise due to its characteristics that align with
the nature of analytical task without the need for a full-fledged data warehouse
or database management system:

1. **In-Memory Analytical Database:**
   - DuckDB is an in-memory analytical database, offering high-speed query
     execution. This makes it suitable for scenarios where quick and efficient
     analysis of data is crucial.

2. **Ease of Use:**
   - DuckDB is known for its ease of use and minimal setup requirements. It
     provides a hassle-free environment for running complex SQL queries without
     the need for extensive configuration.

3. **Embedded Database:**
   - DuckDB can be embedded into applications, offering flexibility in
     deployment scenarios. For the SQL exercise, this means a seamless
     integration into the project without external dependencies.

#### Python Data Engineering

- **Data:**
  - `data/python_and_data_engineer`: Contains the data provided for the Python
    Data Engineering exercise.


- **Notebooks:**
  - `notebooks/python_and_data_engineering_data_exploration.ipynb`: Jupyter
    notebook for exploring and testing solutions related to the Python Data
    Engineering exercise.

- **Source Code:**
  - `src/servier_de_python/pipeline.py`: Apache Beam entrypoint for running the
    data pipeline. Utilizes ArgParser for running locally via Python or another
    runner classically, forwarding all arguments to the PipelineOptions.
  - `src/servier_de_python/io.py`: IO module wrapping implementation details to
    load data according to the exercise format.
  - `src/servier_de_python/transforms.py`: Transformation step, including word
    splitting to match drug names.

##### Running the Pipeline

To execute the data pipeline for the Python and Data Engineering exercise, use the Apache Beam entry point:

```bash
pipx run pdm run src/servier_de_python/pipeline.py \
    --clinical-trials-dataset-path ./data/python_and_data_engineering/clinical_trials.csv \
    --drugs-dataset-path ./data/python_and_data_engineering/drugs.csv \
    --pubmed-dataset-path ./data/python_and_data_engineering/pubmed.csv \
        ./data/python_and_data_engineering/pubmed.json \
    --output ./output/drug-mentions
```

Note: Adjust input and output paths as needed.

#### SQL

- **Data:**
  - `data/sql`: Contains data for the SQL exercise.

- **Notebooks:**
  - `notebooks/sql_data_exploration.ipynb`: Jupyter notebook for exploring and testing solutions related to the SQL exercise.

### Going Further

The choice of Apache Beam for this exercise was driven by several
considerations, and it's worth delving into the nuances of this decision.

#### Data Shuffling and Partitioning

Apache Beam is designed to handle data shuffling and partitioning in a
distributed fashion, making it suitable for scenarios where scalability is
paramount. This is especially relevant for the Python Data Engineering exercise,
where the dataset could be theoretically large and requires efficient
processing. The `CoGroupByKey` transform is used to group the publications by
drug name, this step similar to the stage between the Mapper and the Reducer in
the MapReduce framework.

#### Apache Beam Flexibility and Host Language Integration

While Apache Beam was chosen primarily to revisit and reassess its maturity
since previous projects, and to be completely honest, for fun. Its distinct
advantage lies in its flexibility with host languages. Using either Kotlin or
Python with Apache Beam allows leveraging the full range of capabilities provided
by these languages. This flexibility is crucial, especially when integrating
with various libraries and features essential for the specific requirements of
data pipeline tasks.

#### Task-Specific Performant Data Pipelines

In previous projects, Apache Beam emerged as the preferred framework,
particularly for task-specific data pipelines, such as ML inference. Apache
Beam's reliance on the host language runtime enables seamless integration with
existing libraries, providing a performance edge for specialized tasks. This
becomes especially relevant when compared to Apache Spark, which employs a query
planner and executes computations in a deferred fashion, posing challenges for
specific use cases like ML inference.

#### Apache Beam vs. Apache Spark: Streaming vs. Analytics

It's essential to highlight that Apache Beam originates more from the streaming
processing domain rather than analytics. While Apache Spark offers convenience
for global bounded dataset analytics, Apache Beam excels in scenarios where
streaming and real-time processing are paramount.

#### Consideration for Data Warehouses

Acknowledging the unique characteristics of Apache Beam, it's crucial to note
that for broader analytics on global bounded datasets or considering the
efficiency parameters of this specific project, alternative technologies like
data warehouses (e.g., BigQuery) or headless query execution engines (e.g., AWS
Athena) might be more suitable. These technologies are optimized for analytics
workloads and may provide better performance in certain scenarios.
