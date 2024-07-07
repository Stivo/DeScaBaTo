FROM amazoncorretto:17

RUN yum install -y git

RUN curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
RUN mv sbt-rpm.repo /etc/yum.repos.d/
RUN yum install -y sbt-1.10.0

RUN sbt --version

RUN git clone -b feature/fix_logging https://github.com/stivo/descabato

WORKDIR /descabato

RUN sbt pack

ENTRYPOINT ["/descabato/core/target/pack/bin/descabato"]

# This is optional but does integration tests, this may take a few minutes
# RUN sbt test
