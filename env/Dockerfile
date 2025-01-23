FROM oraclelinux:8 AS oracle8

# ---------------------------------- Installing dependencies as root ---------------------------------- 
RUN dnf install -y wget epel-release git cmake3 gcc-c++ gcc binutils \
compat-openssl10 libX11-devel libXpm-devel libXft-devel libXext-devel \ 
gsl-devel openssl-devel wget bzip2-devel libffi-devel xz-devel sqlite-devel \
ncurses ncurses-devel make xz libzstd libzstd-devel which rsync \
nmap-ncat chrony 

RUN dnf install -y oracle-epel-release-el8
RUN dnf config-manager --enable ol8_codeready_builder
RUN dnf install -y hdf5 hdf5-devel

# ---------------------------------- Create gamma ---------------------------------- 
RUN useradd gamma
USER gamma
WORKDIR /home/gamma
RUN mkdir -p /home/gamma/env
COPY environment.yml /home/gamma/env
COPY requirements.txt /home/gamma/env
SHELL ["/bin/bash", "--login", "-c"]

USER root 

# ARM processors (Mac)
# Definisci la variabile per il file Miniconda
ARG MINICONDA=Miniconda3-latest-Linux-aarch64.sh
# INTEL/AMD processors
ARG MINICONDA=Miniconda3-latest-Linux-x86_64.sh

# Scarica Miniconda utilizzando la variabile
RUN wget https://repo.anaconda.com/miniconda/$MINICONDA \
    && chmod +x $MINICONDA \
    && ./$MINICONDA -b -p /opt/conda \
    && rm $MINICONDA

RUN chown -R gamma:gamma /home/gamma

USER gamma

RUN export PATH=$PATH:/opt/conda/bin && conda config --append channels conda-forge && conda config --set channel_priority strict

#RUN export PATH=$PATH:/opt/conda/bin && conda create -n gamma -f /home/gamma/env/environment.yml

RUN export PATH=$PATH:/opt/conda/bin && conda create -n gamma python=3.12 pip jupyter numpy scipy matplotlib pandas plotly scikit-learn tensorboard tensorflow inotify_simple -y 

USER root
RUN  mkdir /shared_dir
RUN chown -R gamma:gamma /shared_dir
RUN  mkdir /data01
RUN chown -R gamma:gamma /data01
RUN  mkdir /data02
RUN chown -R gamma:gamma /data02
COPY ./entrypoint.sh /home/gamma/entrypoint.sh
RUN chmod +x /home/gamma/entrypoint.sh

USER gamma
RUN mkdir /home/gamma/workspace
#dir to run pipeline
RUN mkdir /home/gamma/workspace/data
RUN mkdir /home/gamma/workspace/data/input
RUN mkdir /home/gamma/workspace/heasarc
RUN mkdir /home/gamma/workspace/log


ENV PATH="/opt/conda/bin:$PATH"


#ENTRYPOINT ["bash", "/home/gamma/entrypoint.sh"]
