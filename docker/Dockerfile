FROM r-base

RUN apt-get update \
  && apt-get install -y --no-install-recommends  \
    ca-certificates \
    curl \
    git \
    libcurl4-openssl-dev \
    libhiredis-dev \
    libssl-dev/unstable \
    libxml2-dev \
    pandoc \
    ssh \
  && apt-get clean

RUN rm /usr/local/bin/install2.r && \
  wget --no-check-certificate \
  https://raw.githubusercontent.com/cboettig/littler/master/examples/install2.r \
  -O /usr/local/bin/install2.r \
  && chmod +x /usr/local/bin/install2.r

RUN install2.r --error  \
    crayon \
    devtools \
    digest \
    docopt \
    downloader \
    dplyr \
    inline \
    R6 \
    RCurl \
    rmarkdown \
    stringr

RUN installGithub.r  \
    ropensci/RedisAPI \
    richfitz/RedisHeartbeat \
    richfitz/storr \
    richfitz/remake \
    gaborcsardi/progress \
    traitecoevo/dockertest \
    traitecoevo/rrqueue

RUN r -e 'remake:::install_remake("/usr/local/bin")'
RUN r -e 'rrqueue:::install_scripts("/usr/local/bin")'

WORKDIR /root

CMD ["bash"]
