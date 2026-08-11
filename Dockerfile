ARG PYTHON_VERSION=3.13
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:${PYTHON_VERSION}
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /tmp
ENV PYTHONPATH=${LAMBDA_TASK_ROOT}

RUN dnf install -y git findutils gcc-c++

ADD . /tmp

RUN <<EOF
uv export --locked --no-editable --no-dev --format requirements.txt -o requirements.txt
uv pip install \
  --compile-bytecode \
  --target "${LAMBDA_TASK_ROOT}" \
  --no-cache-dir \
  --disable-pip-version-check \
  -r requirements.txt
EOF

CMD ["hls_stac_parquet.write_handler.handler"]
