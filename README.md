# Data Pipeline Project

Please do not fork this repository, but use this repository as a template for your refactoring project. Make Pull Requests to your own repository even if you work alone and mark the checkboxes with an x, if you are done with a topic in the pull request message.

## Project for today
The task for today you can find in the [data-pipeline-project.md](data-pipeline-project.md) file.


## Environment

Use this file to create a new environment for this task.

```bash
pyenv local 3.11.3
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Questions:

1. What are the steps you took to complete the project?
    * Created a Jupiter notebook to test the download of the file from the data sorce and to test the calculation of the revenue per day [SQL-with-spark_rev_per_day.ipynb](SQL-with-spark_rev_per_day.ipynb)
    * Created a [data_ingestion.py](src/data_ingestion.py) for data ingestion to the GCS bucket
    * Created a PySpark file [revenue_report.py](src/revenue_report.py) for converting the data by a Dataproc job and saving the results to the GCS bucket
    
2. What are the challenges you faced?
    * Applying the new accomplishments from the whole week of boot camp to a new task was very exciting and thrilling at the same time. The bonus part is very challenging and needs more time. I will try this part again after the bootcamp. 

3. What are the things you would do differently if you had more time?
    * Would like to get into the prefect topic, as workflow orchestration can take a lot of the work out of data science.