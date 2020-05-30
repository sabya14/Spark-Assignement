FROM bde2020/spark-submit:2.4.5-hadoop2.7
ARG SBT_VERSION
ENV SBT_VERSION=${SBT_VERSION:-1.2.6}
RUN wget -O - https://piccolo.link/sbt-1.2.6.tgz | gunzip | tar -x -C /usr/local
ENV PATH /usr/local/sbt/bin:${PATH}

WORKDIR /app

# Pre-install base libraries
ADD build.sbt /app/
ADD project/plugins.sbt /app/project/
RUN sbt update


# Copy the build.sbt first, for separate dependency resolving and downloading
COPY project /app/project


# Copy the source code and build the application
COPY . /app
RUN sbt clean assembly

CMD ["docker/run.sh"]