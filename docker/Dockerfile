FROM python

# Installing latest NewRelicUploader and Taurus as dependency
RUN pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && git clone https://github.com/doctornkz/newrelicUploader.git \
    && cd newrelicUploader \
    && pip install .