# CohEEL
A library for the automatic detection and disambiguation of knowledge base entity mentions in texts.

## Execution

Programs can be run via the `bin/run` script.
All programs need a `--configuration` parameter, which identifies a file under `src/main/resources`.
This file configures required properties, such as job manager, hdfs, path to certain files etc.

### Spread Wikipedia data dump to HDFS

``` sh
bin/spread-wikidump.sh
```

### Run preprocessing and classification scripts

``` sh
# preprocessing: extract main data like surfaces, links, redirects, language models, etc.
bin/run --configuration cluster_tenem --program extract-main

# extract probability that a surface is linked at all
bin/prepare-surface-link-probs-program.sh
bin/run --configuration cluster_tenem --program surface-link-probs

# create training data
bin/prepare-tries.sh
# .. upload tries manually to locations specified in the configuration
bin/run --configuration cluster_tenem --program training-data
# download test and training set
bin/download-training-data.sh
# training
mvn scala:run -Dlauncher=MachineLearningTestSuite

# classification
bin/run --configuration cluster_tenem --program classification --parallelism 10
```

## AWS EMR Setup
To setup CohEEL on Amazon Elastic MapReduce (EMR), a proper installation of the [AWS Command Line Interface](https://aws.amazon.com/cli/) is required. Use `aws configure` to configure the local installation.
Furthermore, you have to setup your EC2 key pair name `[keyname]`, as well as the path to your private key file `[pemfile]`:
``` sh
aws configure set emr.key_name [keyname]
aws configure set emr.key_pair_file [pemfile]
```

The following command starts a cluster (named "coheel") with 20 worker instances of [type](https://aws.amazon.com/ec2/instance-types/) m1.large:
``` sh
# create a new cluster
aws emr create-cluster --name "coheel" \
    --release-label emr-4.2.0 \
    --use-default-roles \
    --applications Name=Hadoop Name=Ganglia \
    --instance-count 21 \
    --instance-type m1.large \
    --configurations '[{ "Classification": "yarn-site", "Properties": { "yarn.nodemanager.resource.cpu-vcores": "1", "yarn.nodemanager.resource.memory-mb": "5120" } }]' \
    --bootstrap-action Name="installFlink",Path="s3://coheel-conf/install-flink-0.10.1.sh"

# wait until the cluster is running and get the name of the master node by executing
aws emr describe-cluster --cluster-id [ClusterId] | grep MasterPublicDnsName | cut -d\" -f4
```

Connect to the master node via ssh (user `hadoop` and identity file `[pemfile]`) and install some required/useful dependencies (Maven, Git, jd, tmux)
``` sh
# install some dependencies (Maven, Git, jd, tmux)
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo && sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo && sudo yum install -y apache-maven
sudo wget http://stedolan.github.io/jq/download/linux64/jq -O /usr/local/sbin/jd ; sudo chmod go+x /usr/local/sbin/jd
sudo yum install tmux git

# start a Apache Flink YARN session on the EMR cluster (using 20 workers)
yarn-session.sh -n 20 -s 1 -jm 768 -tm 4096 -Dfs.overwrite-files=true -Dtaskmanager.memory.fraction=0.5
```

To download and setup CohEEL run:
``` sh
git clone https://github.com/stratosphere/coheel.git
cd coheel
# automatically retrieve the current cluster setup
source bin/load-aws-config.sh
```

Run a CohEEL program as usual (see Execution section) by choosing the `cluster_aws` setup
``` sh
bin/run --configuration cluster_aws --program [...] --parallelism 20 ; coheel_message "CohEEL job finished!"
```
The `coheel_message` method sends an [AWS SNS notification](https://aws.amazon.com/sns/) w/ some details after the program was terminated.

