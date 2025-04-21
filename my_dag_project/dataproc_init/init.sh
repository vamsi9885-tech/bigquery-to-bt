
#!/bin/bash
sudo apt-get update
sudo apt-get install -y python3-pip
pip3 install apache-beam[gcp] google-cloud-bigtable pyarrow
