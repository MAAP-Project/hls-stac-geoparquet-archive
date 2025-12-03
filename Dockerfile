ARG PYTHON_VERSION=3.13
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:${PYTHON_VERSION}
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /tmp

RUN dnf install -y git findutils gcc-c++

ADD . /tmp

RUN <<EOF
uv export --locked --no-editable --no-dev --format requirements.txt -o requirements.txt
uv pip install \
  --compile-bytecode \
  --target /asset \
  --no-cache-dir \
  --disable-pip-version-check \
  -r requirements.txt
EOF

# Reduce package size and remove useless files
WORKDIR /asset
RUN <<EOF
dnf clean all
rm -rf /var/cache/dnf
find . -type f -name '*.pyc' | while read f; do n=$(echo $f | sed 's/__pycache__\///' | sed 's/.cpython-[0-9]*//'); cp $f $n; done;
find . -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf
find . -type f -a -name '*.py' -print0 | xargs -0 rm -f
find . -type d -a -name 'tests' -print0 | xargs -0 rm -rf

find . -type f -name '*.so*' \
  -not -path "./numpy.libs/*" \
  -exec strip --strip-unneeded {} \;
EOF

CMD ["echo", "hello world"]

