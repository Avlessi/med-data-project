### Description

The goal of this project is to find drug mentions according to rules:
- drug is considered as mentioned in PubMed article or in clinical test if it's mentioned in publication title.
- drug is considered as mentioned bu journal if it is mentioned in publication issued by this journal.

Example input files are located in ```resources``` folder.
They are: 
 - clinical_trials.csv
 - drugs.csv
 - pubmed.csv


Additional feature is:
 - find the journal which mentions the biggest number of different drugs

To launch a project, you should run:

```python main.py <clinical_trials_path> <drugs_path> <pubmed_path> <output_path>```

In case if you want to run the job with Airflow, you can
specify these paths in ```application_args```  of SparkSubmitOperator.
  

To run project on input files from resources folder, you can use:
```python main.py resources/clinical_trials.csv resources/drugs.csv resources/pubmed.csv output```

main.py executes a job specified in ```jobs.mentioned_drugs``` folder.
In output we get json file with columns having information like this:
 ```{"drug":"BETAMETHASONE","journal":"Hôpitaux Universitaires de Genève","clinical_trials":"yes","pubmed":"no"}```

An additional feature is implemented in folder ```jobs.best_journal```.

### Tests
Tests are implemented in ```tests``` folder using ```pytest``` framework.
I used it because it has a support of fixture which is pretty useful
for managing long-lived test resources such as SparkSession in our case.
Resources folder for tests is called ```tests_resources```.
To run tests, use command:
```pytest```

### Production
If our goal was to go into production and our input was represented with
billions of lines, then: 
- we could add a step converting csv input files into Delta tables.
It would help us to increase reading speed and have input data schema 
validation checks. It could also be a good data source in case
if we decided to treat data in streaming way.
- if some of input files are less than 10 GB, then we could increase
our broadcast limit to this size in order to make broadcast join
- we would set scheduler mode to FAIR, instead of default FIFO 
so that all jobs get a roughly equal share of cluster resources
- we could play with spark.sql.shuffle.partitions parameter. Its optimal
value usually increases a performance.
- we would activate Kryo serialization
- we would make experiments to find optimal number of executors and partitions 
