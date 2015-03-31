# VERSION 1.0.12
FROM keboola/base-php
MAINTAINER Ondrej Popelka <ondrej.popelka@keboola.com>

RUN yum -y install R
RUN mkdir /usr/share/doc/R-3.1.2/html

# install some commonly used R packages
RUN echo "install.packages(c('corrgram', 'data.table', 'gbm', 'ggplot2', 'jsonlite', 'leaps', 'plyr', 'rJava', 'RJDBC'), repos = 'http://cran.us.r-project.org')" >> /tmp/init.R
RUN Rscript /tmp/init.R

WORKDIR /home

# Initialize 
RUN git clone https://github.com/keboola/docker-lgr-bundle ./
RUN git checkout tags/1.0.0
RUN composer install --no-interaction

ENTRYPOINT php ./app/console lgr:run --data=/data
