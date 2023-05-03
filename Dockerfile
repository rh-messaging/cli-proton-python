# Arguments for DEV's (comment static FROM and uncomnnet #DEV ones)
ARG UBI_VERSION=9
ARG PYTHON_VERSION=60
ARG UBI_BUILD_TAG=latest
ARG UBI_RUNTIME_TAG=latest
ARG IMAGE_BUILD=registry.access.redhat.com/ubi${UBI_VERSION}/python-${PYTHON_VERSION}:${UBI_TAG}
ARG IMAGE_BASE=registry.access.redhat.com/ubi${UBI_VERSION}/python-${PYTHON_VERSION}:${UBI_RUNTIME_TAG}

#DEV FROM $IMAGE_BUILD
FROM registry.access.redhat.com/ubi9/python-39:1-114.1683012551

LABEL name="Red Hat Messaging QE - Proton Python CLI Image" \
      run="podman run --rm -ti <image_name:tag> /bin/bash cli-proton-python-*"

USER root

# install fallocate for use by claire tests
RUN dnf -y --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install \
    util-linux \
    && dnf clean all -y

COPY . /src
WORKDIR /src

# install the client and its dependencies
RUN python3 -m pip install --editable .

RUN mkdir /var/lib/cli-proton-python && \
    chown -R 1001:0 /var/lib/cli-proton-python  && \
    chmod -R g=u /var/lib/cli-proton-python

USER 1001

VOLUME /var/lib/cli-proton-python
WORKDIR /var/lib/cli-proton-python

CMD ["/bin/bash"]
