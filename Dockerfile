FROM keboola/base-php
MAINTAINER Ondrej Popelka <ondrej.popelka@keboola.com>

RUN yum -y install R
# Create html folder under the R directory (name of directory depends on version)
RUN find /usr/share/doc/ -name R* -exec mkdir '{}/html' \;

# install some commonly used R packages
RUN echo "install.packages(c('corrgram', 'data.table', 'gbm', 'ggplot2', 'jsonlite', 'leaps', 'plyr', 'rJava', 'RJDBC'), repos = 'http://cran.us.r-project.org')" >> /tmp/init.R
RUN Rscript /tmp/init.R

WORKDIR /home

# Initialize 
RUN git clone https://github.com/keboola/docker-lgr-bundle ./
RUN composer install --no-interaction

ENTRYPOINT php ./app/console lgr:run --data=/data
