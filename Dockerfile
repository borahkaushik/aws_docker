FROM ubuntu:18.04

# RUN . /etc/os-release

RUN apt update
RUN apt install -y wget
RUN apt install -y python3.7
RUN apt install -y python3-pip
RUN python3.7 -m pip install pip==21.0.1
RUN apt install -y libpython3.7-dev

# Requirement for app requirement
COPY requirements.txt /app/requirements.txt
RUN cat /app/requirements.txt | xargs -L 1 python3.7 -m pip install

# Requirement specific for neuron containers
COPY neuron_torch_requirement.txt /app/neuron_torch_requirement.txt
RUN python3.7 -m pip install -r /app/neuron_torch_requirement.txt

COPY main_container/ /app/
COPY ml_utils /app/ml_utils/

WORKDIR /app
