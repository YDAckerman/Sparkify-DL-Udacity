
# Project Summary

This is an educational project for learning how to provision and use AWS
EMR resources. More broadly it is an exercise in building data
pipelines with Spark. The project uses simulated
music-streaming data hosted by Udacity to create a data lake for
an imaginary company called Sparkify. The major steps in creating this
project were: 

    - prototyping the data pipeline with pyspark in a jupyter
    notebook
    - transcribing the prototype to python
    - provisioning emr resources with terraform
    - moving the pipeline scripts to the emr master node
    - submitting and debugging spark-jobs until success was achieved

# Next Steps

    - Provide Data Quality Checks.
    - Improve pipeline efficiency by combining the two etl functions.
    - Improve terraform script.

# Running the Script(s)

Provision the EMR cluster with:
    
    'cd resources'
    'terraform apply'

Get the host address from the aws console and use it to fill in
spark_cluster_ssh and spark_cluster_scp.

Zip python-etl with:
    
    'zip -r etl.zip python-etl/'
    
And send it to the master node with:

    'spark_cluster_scp.sh'
    
ssh into the master node with:

    'spark_cluster_ssh.sh'
    
and unzip the pipeline with:

    'unzip etl.zip'
    
finally, cd into python-etl and run the spark job with:

    'spark-submit etl.py'

# Repository Contents

- README.md
    - this right here
- python-etl/
    - etl.py: run with spark-submit on EMR master node
    - dl.cfg: config file for aws access id/key
- resources/
    - main.tf: provision emr with appropriate applications and access
    - spark-cluster.pem: key for accessing EMR master node
    - spark_cluster_ssh.sh: 1-line script to ssh into the remote machine
    - spark_cluster_scp.sh: 1-line script to scp the pipeline to the remote machine
