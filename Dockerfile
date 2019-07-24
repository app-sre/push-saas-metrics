FROM centos:7

RUN yum install -y epel-release && \
    yum install -y python2-pip git && \
    pip install --upgrade pip setuptools && \
    yum clean all

ADD *.py /push-saas-metrics/
ADD requirements.txt /push-saas-metrics/

WORKDIR  /push-saas-metrics

RUN pip install -r requirements.txt

CMD ["python", "push-saas-metrics.py"]
