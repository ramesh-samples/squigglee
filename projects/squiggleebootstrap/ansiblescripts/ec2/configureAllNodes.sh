#!/bin/bash
for i in `seq 0 100`;
do
        /usr/local/bin/ansible-playbook tsr_ec2_configure.yml --extra-vars logicalNumber=$i &
done