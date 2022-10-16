ARG SITE_PACKAGES=/opt/spark/work-dir
FROM python:3.7-slim AS prepare-image
ARG SITE_PACKAGES
#ARG INDEX_URL
#ENV PIP_INDEX_URL=${INDEX_URL}
RUN mkdir -p ${SITE_PACKAGES}
RUN pip3 install --upgrade pip
ADD requirements.txt requirements.txt
RUN pip3 install --prefix=${SITE_PACKAGES} -r requirements.txt

FROM gcr.io/spark-operator/spark-py:v3.0.0-hadoop3
ARG SITE_PACKAGES
COPY --from=prepare-image ${SITE_PACKAGES}/lib/python3.7/site-packages /usr/lib/python3/dist-packages
COPY ./ /opt/spark
USER 0:0
RUN ln -sf /usr/bin/python3 /usr/bin/python
